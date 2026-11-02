// Self-test for mlog_spool.h: CRC-32C check value, record layout, recovery
// of a torn tail, and GAP records when the ring overflows.
#include "mlog_spool.h"

#include <cstdio>
#include <cstdlib>
#include <string>

#define CHECK(cond)                                                    \
  do {                                                                 \
    if (!(cond)) {                                                     \
      std::fprintf(stderr, "%s:%d: CHECK(%s) failed\n", __FILE__, __LINE__, #cond); \
      std::exit(1);                                                    \
    }                                                                  \
  } while (0)

int main(int argc, char** argv) {
  std::string dir = argc > 1 ? argv[1] : "/tmp";
  // Standard CRC-32C check value.
  const char* check = "123456789";
  CHECK(mlog::crc32c(reinterpret_cast<const uint8_t*>(check), 9) == 0xE3069283u);

  std::vector<uint8_t> rec;
  const uint8_t payload[3] = {1, 2, 3};
  mlog::encode_record(rec, 1, 7, 42, 5, -9, payload, 3);
  CHECK(rec.size() == mlog::kHeaderSize + 3 + mlog::kTrailerSize);
  CHECK(rec[0] == 1 && rec[1] == 1 && rec[2] == 7 && rec[4] == 3 && rec[16] == 42 && rec[24] == 5);

  // A torn final record is cut off on reopen and the sequence continues.
  std::string path = dir + "/torn.spool";
  std::remove(path.c_str());
  {
    mlog::SpoolWriter w(path, 9);
    CHECK(w.ok());
    for (int i = 0; i < 3; i++) w.append(1, 1, i, payload, 3);
    CHECK(w.flush() == 0);
  }
  {
    FILE* f = std::fopen(path.c_str(), "ab");
    std::fwrite(rec.data(), 1, 20, f);  // half a record
    std::fclose(f);
  }
  {
    mlog::SpoolWriter w(path, 9);
    CHECK(w.ok() && w.next_seq() == 4);
  }

  // Overflowing a tiny ring produces GAP records with the drop counts.
  std::string gp = dir + "/gap.spool";
  std::remove(gp.c_str());
  uint64_t dropped = 0;
  {
    mlog::AsyncSpool<16, 8> s(gp, 1);
    CHECK(s.ok());
    for (int i = 0; i < 10000; i++)
      if (!s.try_publish(1, 1, i, payload, 3)) dropped++;
    s.close();
    auto st = s.stats();
    CHECK(st.published + st.dropped == 10000 && st.dropped == dropped && st.write_errors == 0);
    CHECK(st.written == st.published + st.gaps);
    std::printf("published=%llu dropped=%llu gaps=%llu\n", (unsigned long long)st.published,
                (unsigned long long)st.dropped, (unsigned long long)st.gaps);
  }
  std::printf("ok\n");
  return 0;
}
