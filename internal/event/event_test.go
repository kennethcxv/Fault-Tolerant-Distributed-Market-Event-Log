package event

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"testing/quick"
)

func sample() Event {
	return Event{Schema: 1, Type: 7, Offset: 42, ProducerID: 3, ProducerSeq: 9,
		SourceTS: 1_700_000_000_000_000_000, IngestTS: -5, Payload: []byte("bid 101.25 x 300")}
}

func TestRoundTripProperty(t *testing.T) {
	f := func(schema uint8, typ uint16, off, pid, seq uint64, src, ing int64, payload []byte) bool {
		in := Event{Schema: schema, Type: typ, Offset: off, ProducerID: pid, ProducerSeq: seq,
			SourceTS: src, IngestTS: ing, Payload: payload}
		buf := in.AppendTo([]byte("prefix"))[len("prefix"):]
		out, n, err := Decode(buf)
		if err != nil || n != len(buf) || n != in.EncodedLen() {
			return false
		}
		if len(in.Payload) == 0 {
			in.Payload, out.Payload = nil, nil
		}
		return reflect.DeepEqual(in, out)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

// CRC32C detects every single-bit error; check that we actually verify it over
// the whole record, header and trailer included.
func TestEverySingleBitFlipDetected(t *testing.T) {
	e := sample()
	rec := e.AppendTo(nil)
	for bit := 0; bit < len(rec)*8; bit++ {
		bad := bytes.Clone(rec)
		bad[bit/8] ^= 1 << (bit % 8)
		if _, _, err := Decode(bad); err == nil {
			t.Fatalf("bit %d flip not detected", bit)
		}
	}
}

func TestTruncationIsReportedAsTruncated(t *testing.T) {
	e := sample()
	rec := e.AppendTo(nil)
	for n := 0; n < len(rec); n++ {
		if _, _, err := Decode(rec[:n]); !errors.Is(err, ErrTruncated) {
			t.Fatalf("prefix %d: got %v, want ErrTruncated", n, err)
		}
	}
}

func TestImpossibleLengthIsCorruption(t *testing.T) {
	e := sample()
	rec := e.AppendTo(nil)
	rec[7] = 0xFF // payload length high byte -> far above MaxPayload
	if _, _, err := Decode(rec); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
}

func TestValidate(t *testing.T) {
	ok := sample()
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, e := range map[string]Event{
		"type 0":   {Schema: 1},
		"schema 0": {Type: 1},
		"too big":  {Schema: 1, Type: 1, Payload: make([]byte, MaxPayload+1)},
	} {
		if err := e.Validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

func FuzzDecode(f *testing.F) {
	e := sample()
	f.Add(e.AppendTo(nil))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, buf []byte) {
		ev, n, err := Decode(buf)
		if err != nil {
			return
		}
		if again := ev.AppendTo(nil); !bytes.Equal(again, buf[:n]) {
			t.Fatalf("decoded record does not re-encode to its input")
		}
	})
}
