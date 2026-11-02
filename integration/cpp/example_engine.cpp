// A stand-in for Project 1's market-data engine: it produces schema-v1
// order-book messages on one thread and hands each to AsyncSpool. It reports
// what the hot path cost (try_publish latency) and what reached the spool.
//
//   example_engine SPOOL PRODUCER_ID EVENTS [RATE | tiny]
//
// RATE paces publishing at that many events per second (0 or absent: as fast
// as possible). "tiny" uses an 8-slot ring so that drops, and GAP records,
// are certain.
#include "mlog_spool.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {

// Schema v1 payloads (docs/integration.md), little-endian.
struct Writer {
  uint8_t buf[64];
  uint32_t n = 0;
  void u8(uint8_t v) { buf[n++] = v; }
  void u32(uint32_t v) { mlog::detail::put_u32(buf + n, v); n += 4; }
  void u64(uint64_t v) { mlog::detail::put_u64(buf + n, v); n += 8; }
};

constexpr uint16_t kAdd = 1, kCancel = 2, kExecute = 3;

template <typename Spool>
int run(Spool& spool, uint64_t events, double rate) {
  auto start = std::chrono::steady_clock::now();
  uint64_t rng = 88172645463325252ull;  // xorshift64: deterministic feed
  auto next = [&] { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng; };
  std::vector<uint64_t> live;
  std::vector<uint32_t> lat_ns;
  lat_ns.reserve(events);
  uint64_t order_id = 0;
  for (uint64_t i = 0; i < events; i++) {
    Writer w;
    uint16_t type;
    uint32_t inst = uint32_t(next() % 4);
    if (live.size() < 64 || next() % 100 < 50) {
      type = kAdd;
      uint8_t side = uint8_t(next() % 2);
      int64_t px = 10000 + int64_t(inst) * 500 + (side ? 1 : -1) * int64_t(1 + next() % 20);
      w.u32(inst); w.u64(++order_id); w.u8(side); w.u64(uint64_t(px)); w.u64(100 * (1 + next() % 10));
      live.push_back(order_id);
    } else {
      size_t k = next() % live.size();
      uint64_t id = live[k];
      live[k] = live.back();
      live.pop_back();
      // Instrument is not tracked here, so these may be rejected by the book;
      // that is fine for a transport test and is deterministic.
      if (next() % 2) { type = kCancel; w.u32(inst); w.u64(id); }
      else { type = kExecute; w.u32(inst); w.u64(id); w.u64(100); }
    }
    if (rate > 0) {  // busy-wait to the paced send time, as a feed handler would
      auto due = start + std::chrono::nanoseconds(int64_t(double(i) * 1e9 / rate));
      while (std::chrono::steady_clock::now() < due) {
      }
    }
    auto t0 = std::chrono::steady_clock::now();
    spool.try_publish(1, type, int64_t(i), w.buf, w.n);
    lat_ns.push_back(uint32_t(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count()));
  }
  double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
  spool.close();
  auto st = spool.stats();
  std::printf("events=%llu seconds=%.3f offered_rate=%.0f/s\n", (unsigned long long)events, secs, double(events) / secs);
  std::sort(lat_ns.begin(), lat_ns.end());
  auto pct = [&](double p) { return lat_ns[std::min(lat_ns.size() - 1, size_t(p * lat_ns.size()))]; };
  std::printf("published=%llu dropped=%llu written=%llu gaps=%llu write_errors=%llu\n",
              (unsigned long long)st.published, (unsigned long long)st.dropped, (unsigned long long)st.written,
              (unsigned long long)st.gaps, (unsigned long long)st.write_errors);
  std::printf("try_publish_ns p50=%u p99=%u p99.9=%u max=%u\n", pct(0.5), pct(0.99), pct(0.999), lat_ns.back());
  return st.write_errors == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 4) {
    std::fprintf(stderr, "usage: %s SPOOL PRODUCER_ID EVENTS [tiny]\n", argv[0]);
    return 2;
  }
  uint64_t producer = std::strtoull(argv[2], nullptr, 10);
  uint64_t events = std::strtoull(argv[3], nullptr, 10);
  if (argc > 4 && std::strcmp(argv[4], "tiny") == 0) {
    mlog::AsyncSpool<240, 8> spool(argv[1], producer);
    if (!spool.ok()) { std::perror("open spool"); return 1; }
    return run(spool, events, 0);
  }
  double rate = argc > 4 ? std::strtod(argv[4], nullptr) : 0;
  mlog::AsyncSpool<> spool(argv[1], producer);
  if (!spool.ok()) { std::perror("open spool"); return 1; }
  return run(spool, events, rate);
}
