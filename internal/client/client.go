// Package client is the Go client for the log's HTTP API. It follows leader
// redirects, retries writes through leader changes, and verifies record
// checksums on reads.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/api"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
)

type Client struct {
	endpoints []string
	hc        *http.Client

	mu     sync.Mutex
	leader string
	next   int
}

// New takes base URLs ("http://127.0.0.1:7001") or bare host:port pairs.
func New(endpoints ...string) *Client {
	c := &Client{hc: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64}}}
	for _, e := range endpoints {
		if !strings.Contains(e, "://") {
			e = "http://" + e
		}
		c.endpoints = append(c.endpoints, strings.TrimRight(e, "/"))
	}
	return c
}

func (c *Client) Endpoints() []string { return c.endpoints }

// Append submits a batch and retries until it is applied or rejected, or ctx
// ends. Retrying is safe because producer sequence numbers make the append
// idempotent: if an earlier attempt committed, the retry returns
// StatusDuplicate. A returned error with code outcome_unknown means ctx ended
// before the outcome was learned; retry with the same sequence numbers.
func (c *Client) Append(ctx context.Context, req api.AppendRequest) (fsm.Result, error) {
	var res fsm.Result
	err := c.write(ctx, "/v1/append", req, &res)
	return res, err
}

// Checkpoint stores a consumer's offset. Retries are safe because checkpoints
// only move forward.
func (c *Client) Checkpoint(ctx context.Context, consumer string, offset uint64) (fsm.Result, error) {
	var res fsm.Result
	err := c.write(ctx, "/v1/consumers/"+consumer+"/checkpoint", api.CheckpointRequest{Offset: offset}, &res)
	return res, err
}

// AppendOnce makes one attempt against one endpoint, with no retries or
// redirects. Tests use it to record exact client histories.
func (c *Client) AppendOnce(ctx context.Context, endpoint string, req api.AppendRequest) (fsm.Result, error) {
	var res fsm.Result
	err := c.post(ctx, endpoint, "/v1/append", req, &res)
	return res, err
}

func (c *Client) write(ctx context.Context, path string, body, out any) error {
	backoff := 10 * time.Millisecond
	// mayHaveApplied records whether any attempt could have taken effect; if
	// so, giving up must report outcome_unknown rather than the last error.
	mayHaveApplied := false
	var lastErr error
	for attempt := 0; ; attempt++ {
		target := c.target()
		err := c.post(ctx, target, path, body, out)
		if err == nil {
			return nil
		}
		lastErr = err
		var apiErr *api.Error
		if errors.As(err, &apiErr) {
			switch apiErr.Code {
			case api.CodeNotLeader:
				if apiErr.LeaderURL != "" && apiErr.LeaderURL != target {
					c.setLeader(apiErr.LeaderURL)
					continue
				}
			case api.CodeOutcomeUnknown, api.CodeTimeout:
				mayHaveApplied = true
			case api.CodeNoLeader, api.CodeOverloaded, api.CodeNodeFailed:
			default:
				return err // not retryable: invalid request, etc.
			}
		} else if opErr := (*net.OpError)(nil); !errors.As(err, &opErr) || opErr.Op != "dial" {
			// Only a failed dial proves the request never reached a server;
			// any other transport error may hit after the server received it.
			mayHaveApplied = true
		}
		c.forgetLeader(target)
		select {
		case <-ctx.Done():
			if !mayHaveApplied {
				return lastErr
			}
			return &api.Error{Code: api.CodeOutcomeUnknown, Message: fmt.Sprintf("gave up after %d attempts: %v", attempt+1, lastErr)}
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, time.Second)
	}
}

// Read returns up to limit verified events starting at from, and the
// serving node's last offset. linearizable=true guarantees the result
// reflects every append acknowledged before the call.
func (c *Client) Read(ctx context.Context, from uint64, limit int, linearizable bool) ([]event.Event, uint64, error) {
	path := fmt.Sprintf("/v1/events?format=raw&from=%d&limit=%d", from, limit)
	if linearizable {
		path += "&linearizable=true"
	}
	var lastErr error
	for i := range c.endpoints {
		target := c.endpoints[i]
		if i == 0 && linearizable {
			target = c.target()
		}
		resp, err := c.get(ctx, target, path)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				break
			}
			continue
		}
		raw, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		last, _ := strconv.ParseUint(resp.Header.Get("X-Last-Offset"), 10, 64)
		events, err := event.DecodeAll(raw)
		if err != nil {
			return nil, 0, fmt.Errorf("client: response from %s failed verification: %w", target, err)
		}
		for j, e := range events {
			if e.Offset != from+uint64(j) {
				return nil, 0, fmt.Errorf("client: %s returned offset %d, expected %d", target, e.Offset, from+uint64(j))
			}
		}
		return events, last, nil
	}
	return nil, 0, lastErr
}

type Producer struct {
	ProducerID uint64 `json:"producer_id"`
	LastSeq    uint64 `json:"last_seq"`
	LastOffset uint64 `json:"last_offset"`
}

// Producer returns a producer's last committed sequence (linearizable).
func (c *Client) Producer(ctx context.Context, id uint64) (Producer, error) {
	var p Producer
	err := c.getJSON(ctx, fmt.Sprintf("/v1/producers/%d?linearizable=true", id), &p)
	return p, err
}

func (c *Client) Consumer(ctx context.Context, name string) (api.ConsumerResponse, error) {
	var r api.ConsumerResponse
	err := c.getJSON(ctx, "/v1/consumers/"+name+"?linearizable=true", &r)
	return r, err
}

// Status queries one specific node.
func (c *Client) Status(ctx context.Context, endpoint string) (api.Status, error) {
	var s api.Status
	resp, err := c.get(ctx, endpoint, "/v1/status")
	if err != nil {
		return s, err
	}
	defer resp.Body.Close()
	return s, json.NewDecoder(resp.Body).Decode(&s)
}

// getJSON tries the leader first, then every endpoint.
func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	var lastErr error
	targets := append([]string{c.target()}, c.endpoints...)
	for _, t := range targets {
		resp, err := c.get(ctx, t, path)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return err
			}
			continue
		}
		err = json.NewDecoder(resp.Body).Decode(out)
		resp.Body.Close()
		return err
	}
	return lastErr
}

func (c *Client) get(ctx context.Context, base, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, decodeError(resp)
	}
	return resp, nil
}

// post sends one request. Responses carrying a state-machine result (even a
// rejection such as sequence_gap) decode into out; API errors return *api.Error.
func (c *Client) post(ctx context.Context, base, path string, body, out any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var probe struct {
		Code   string `json:"code"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return fmt.Errorf("client: %s: HTTP %d: %s", base, resp.StatusCode, bytes.TrimSpace(data))
	}
	if probe.Status != "" {
		return json.Unmarshal(data, out)
	}
	apiErr := &api.Error{}
	json.Unmarshal(data, apiErr)
	return apiErr
}

func decodeError(resp *http.Response) error {
	data, _ := io.ReadAll(resp.Body)
	apiErr := &api.Error{}
	if json.Unmarshal(data, apiErr) != nil || apiErr.Code == "" {
		return fmt.Errorf("client: HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(data))
	}
	return apiErr
}

func (c *Client) target() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != "" {
		return c.leader
	}
	t := c.endpoints[c.next%len(c.endpoints)]
	c.next++
	return t
}

func (c *Client) setLeader(url string) {
	c.mu.Lock()
	c.leader = url
	c.mu.Unlock()
}

func (c *Client) forgetLeader(url string) {
	c.mu.Lock()
	if c.leader == url {
		c.leader = ""
	}
	c.mu.Unlock()
}
