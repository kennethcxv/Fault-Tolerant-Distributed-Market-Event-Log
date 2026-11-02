// mlog_spool.h: hands market events from a latency-critical engine to the
// durable event log without putting the log on the engine's hot path.
//
// The engine thread calls AsyncSpool::try_publish, which copies the event
// into a fixed-size single-producer/single-consumer ring and returns. It
// never blocks, allocates or makes a system call. A background thread drains
// the ring into an append-only spool file in the log's v1 envelope
// (docs/design.md §3). A separate process, mlog-publisher, tails that file
// into the replicated log.
//
// If the ring is full (the disk cannot keep up), try_publish returns false
// and counts the event as dropped. The writer thread then emits an explicit
// GAP record carrying the number of events lost, so the loss is visible in
// the log instead of silent.
//
// Header-only, C++17, POSIX. No dependencies.
#pragma once

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace mlog {

constexpr uint8_t kFormatV1 = 1;
constexpr size_t kHeaderSize = 48;
constexpr size_t kTrailerSize = 4;
constexpr uint16_t kTypeGap = 0xFFFF;

// CRC-32C (Castagnoli), reflected polynomial 0x82F63B78, as in Go's
// hash/crc32 with crc32.Castagnoli. Table-driven; fast enough for the writer
// thread, which is not latency-critical.
inline uint32_t crc32c(const uint8_t* data, size_t n, uint32_t crc = 0) {
  static const auto table = [] {
    std::vector<uint32_t> t(256);
    for (uint32_t i = 0; i < 256; i++) {
      uint32_t c = i;
      for (int k = 0; k < 8; k++) c = (c & 1) ? (c >> 1) ^ 0x82F63B78u : c >> 1;
      t[i] = c;
    }
    return t;
  }();
  crc = ~crc;
  for (size_t i = 0; i < n; i++) crc = table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
  return ~crc;
}

namespace detail {
inline void put_u16(uint8_t* p, uint16_t v) { for (int i = 0; i < 2; i++) p[i] = uint8_t(v >> (8 * i)); }
inline void put_u32(uint8_t* p, uint32_t v) { for (int i = 0; i < 4; i++) p[i] = uint8_t(v >> (8 * i)); }
inline void put_u64(uint8_t* p, uint64_t v) { for (int i = 0; i < 8; i++) p[i] = uint8_t(v >> (8 * i)); }
inline uint32_t get_u32(const uint8_t* p) {
  return uint32_t(p[0]) | uint32_t(p[1]) << 8 | uint32_t(p[2]) << 16 | uint32_t(p[3]) << 24;
}
inline uint64_t get_u64(const uint8_t* p) {
  uint64_t v = 0;
  for (int i = 7; i >= 0; i--) v = v << 8 | p[i];
  return v;
}
}  // namespace detail

// encode_record appends one v1 record. The log assigns the offset, so it is
// zero here; producer_seq is the record's 1-based position in the spool.
inline void encode_record(std::vector<uint8_t>& out, uint8_t schema, uint16_t type, uint64_t producer_id,
                          uint64_t producer_seq, int64_t source_ts, const uint8_t* payload, uint32_t len) {
  size_t start = out.size();
  out.resize(start + kHeaderSize + len + kTrailerSize);
  uint8_t* p = out.data() + start;
  p[0] = kFormatV1;
  p[1] = schema;
  detail::put_u16(p + 2, type);
  detail::put_u32(p + 4, len);
  detail::put_u64(p + 8, 0);  // offset
  detail::put_u64(p + 16, producer_id);
  detail::put_u64(p + 24, producer_seq);
  detail::put_u64(p + 32, uint64_t(source_ts));
  detail::put_u64(p + 40, 0);  // ingest timestamp, set by the log's leader
  if (len) std::memcpy(p + kHeaderSize, payload, len);
  detail::put_u32(p + kHeaderSize + len, crc32c(p, kHeaderSize + len));
}

// SpoolWriter appends records to the spool file. append() only encodes into
// a buffer; flush() writes everything buffered with one write() call, which
// is what lets the writer thread keep up (one syscall per record could not).
// Opening an existing spool scans it to continue the sequence and cuts off a
// torn final record left by a crash mid-write; a corrupt record before the
// end is refused.
class SpoolWriter {
 public:
  SpoolWriter(const std::string& path, uint64_t producer_id) : producer_id_(producer_id) {
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) { error_ = errno; return; }
    recover();
  }
  ~SpoolWriter() { if (fd_ >= 0) ::close(fd_); }
  SpoolWriter(const SpoolWriter&) = delete;
  SpoolWriter& operator=(const SpoolWriter&) = delete;

  // ok is false if the file could not be opened or holds corrupt records.
  bool ok() const { return fd_ >= 0 && error_ == 0; }
  int error() const { return error_; }
  uint64_t next_seq() const { return next_seq_ + pending_; }

  void append(uint8_t schema, uint16_t type, int64_t source_ts, const uint8_t* payload, uint32_t len) {
    encode_record(buf_, schema, type, producer_id_, next_seq_ + pending_, source_ts, payload, len);
    pending_++;
  }

  // flush writes the buffered records and returns how many were lost (0 on
  // success). If a write fails partway (disk full), records that were
  // written completely stay: a reader tailing the file may already have
  // published them, so their sequence numbers must never be reused. Only the
  // torn final record is cut off, and only the records not written count as
  // lost. The file therefore never holds a torn record before later ones.
  uint64_t flush() {
    uint64_t n = pending_;
    pending_ = 0;
    if (n == 0) return 0;
    size_t written = 0;
    bool ok = write_all(buf_.data(), buf_.size(), &written);
    size_t keep = 0;
    uint64_t kept = 0;
    while (kept < n) {
      size_t len = kHeaderSize + detail::get_u32(&buf_[keep + 4]) + kTrailerSize;
      if (keep + len > written) break;
      keep += len;
      kept++;
    }
    size_ += off_t(keep);
    next_seq_ += kept;
    if (!ok && (::ftruncate(fd_, size_) != 0 || ::lseek(fd_, size_, SEEK_SET) < 0)) {
      error_ = errno;  // cannot restore a clean tail: stop writing
    }
    buf_.clear();
    return n - kept;
  }

  // sync makes everything flushed so far durable. The engine host's power
  // loss is the one failure the spool alone cannot survive; call this as
  // often as that risk is worth (see docs/integration.md).
  bool sync() { return ::fsync(fd_) == 0; }

 private:
  void recover() {
    struct stat st;
    if (::fstat(fd_, &st) != 0) { error_ = errno; return; }
    std::vector<uint8_t> data(size_t(st.st_size));
    if (!data.empty() && ::pread(fd_, data.data(), data.size(), 0) != ssize_t(data.size())) { error_ = EIO; return; }
    size_t pos = 0;
    while (pos + kHeaderSize <= data.size()) {
      uint32_t len = detail::get_u32(&data[pos + 4]);
      size_t total = kHeaderSize + size_t(len) + kTrailerSize;
      if (pos + total > data.size()) break;  // torn tail
      if (crc32c(&data[pos], total - kTrailerSize) != detail::get_u32(&data[pos + total - kTrailerSize]) ||
          detail::get_u64(&data[pos + 24]) != next_seq_) {
        error_ = EILSEQ;  // corruption before the end: refuse to append after it
        return;
      }
      next_seq_++;
      pos += total;
    }
    size_ = off_t(pos);
    if (pos != data.size() && ::ftruncate(fd_, size_) != 0) { error_ = errno; return; }
    if (::lseek(fd_, size_, SEEK_SET) < 0) error_ = errno;
  }

  bool write_all(const uint8_t* p, size_t n, size_t* written) {
    *written = 0;
    if (error_ != 0) return false;
    while (n > 0) {
      ssize_t w = ::write(fd_, p, n);
      if (w < 0) {
        if (errno == EINTR) continue;
        return false;
      }
      p += w;
      n -= size_t(w);
      *written += size_t(w);
    }
    return true;
  }

  int fd_ = -1;
  int error_ = 0;
  uint64_t producer_id_;
  uint64_t next_seq_ = 1;  // sequence of the first record not yet written
  uint64_t pending_ = 0;   // records buffered by append
  off_t size_ = 0;         // file size after the last good flush
  std::vector<uint8_t> buf_;
};

// AsyncSpool connects the engine thread to a SpoolWriter through a bounded
// lock-free SPSC ring. Exactly one thread may call try_publish.
template <size_t SlotPayload = 240, size_t Capacity = 1 << 16>
class AsyncSpool {
  static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");

 public:
  struct Stats {
    uint64_t published;      // accepted by try_publish
    uint64_t dropped;        // rejected because the ring was full
    uint64_t written;        // records written to the spool, GAP records included
    uint64_t gaps;           // GAP records written
    uint64_t write_errors;   // writes that failed (disk full, I/O error)
  };

  AsyncSpool(const std::string& path, uint64_t producer_id, bool fsync_each_batch = false)
      : writer_(path, producer_id), fsync_(fsync_each_batch), slots_(Capacity) {
    if (writer_.ok()) thread_ = std::thread([this] { drain(); });
  }
  ~AsyncSpool() { close(); }

  bool ok() const { return writer_.ok(); }

  // try_publish copies the event into the ring. It returns false, and counts
  // a drop, if the ring is full or the payload does not fit a slot.
  bool try_publish(uint8_t schema, uint16_t type, int64_t source_ts, const uint8_t* payload, uint32_t len) {
    size_t head = head_.load(std::memory_order_relaxed);
    if (len > SlotPayload || head - tail_.load(std::memory_order_acquire) == Capacity) {
      dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    Slot& s = slots_[head & (Capacity - 1)];
    s.schema = schema;
    s.type = type;
    s.source_ts = source_ts;
    s.len = len;
    std::memcpy(s.payload, payload, len);
    head_.store(head + 1, std::memory_order_release);
    published_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  // close drains what is queued, stops the writer thread and syncs the file.
  void close() {
    if (thread_.joinable()) {
      stop_.store(true, std::memory_order_release);
      thread_.join();
      writer_.sync();
    }
  }

  Stats stats() const {
    return {published_.load(), dropped_.load(), written_.load(), gaps_.load(), write_errors_.load()};
  }

 private:
  struct Slot {
    int64_t source_ts;
    uint32_t len;
    uint16_t type;
    uint8_t schema;
    uint8_t payload[SlotPayload];
  };

  void drain() {
    uint64_t reported_drops = 0;
    for (;;) {
      bool stopping = stop_.load(std::memory_order_acquire);
      size_t tail = tail_.load(std::memory_order_relaxed);
      size_t head = head_.load(std::memory_order_acquire);
      // Record drops before the events that follow them, so the gap sits at
      // the right place in the sequence.
      uint64_t drops = dropped_.load(std::memory_order_relaxed);
      uint64_t gap = drops - reported_drops;
      if (gap > 0) {
        uint8_t p[8];
        detail::put_u64(p, gap);
        writer_.append(1, kTypeGap, 0, p, 8);
      }
      for (size_t t = tail; t != head; t++) {
        const Slot& s = slots_[t & (Capacity - 1)];
        writer_.append(s.schema, s.type, s.source_ts, s.payload, s.len);
      }
      uint64_t events = head - tail;
      uint64_t batch = (gap > 0) + events;
      // Free the slots before the write: the data is already copied out.
      tail_.store(head, std::memory_order_release);
      if (batch > 0) {
        uint64_t lost = writer_.flush();
        uint64_t kept = batch - lost;
        written_.fetch_add(kept, std::memory_order_relaxed);
        if (gap > 0 && kept > 0) {  // the GAP record is first in the batch
          gaps_.fetch_add(1, std::memory_order_relaxed);
          reported_drops = drops;
        }
        if (lost > 0) {
          // Lost events are reported by the next GAP record, along with any
          // drops a lost GAP record was meant to report.
          write_errors_.fetch_add(1, std::memory_order_relaxed);
          dropped_.fetch_add(std::min(lost, events), std::memory_order_relaxed);
        } else if (fsync_) {
          writer_.sync();
        }
      }
      if (stopping && head == head_.load(std::memory_order_acquire)) return;
      if (batch == 0) std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
  }

  SpoolWriter writer_;
  bool fsync_;
  std::vector<Slot> slots_;
  alignas(64) std::atomic<size_t> head_{0};
  alignas(64) std::atomic<size_t> tail_{0};
  alignas(64) std::atomic<uint64_t> dropped_{0};
  std::atomic<uint64_t> published_{0}, written_{0}, gaps_{0}, write_errors_{0};
  std::atomic<bool> stop_{false};
  std::thread thread_;
};

}  // namespace mlog
