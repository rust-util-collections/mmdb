#include <algorithm>
#include <array>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

using Clock = std::chrono::steady_clock;
using Seconds = std::chrono::duration<double>;

constexpr size_t kMiB = 1u << 20;

void check(const rocksdb::Status& status) {
  if (!status.ok()) {
    std::fprintf(stderr, "%s\n", status.ToString().c_str());
    std::exit(1);
  }
}

std::array<char, 8> encode_key(uint64_t number) {
  std::array<char, 8> bytes{};
  for (int i = 7; i >= 0; --i) {
    bytes[i] = static_cast<char>(number);
    number >>= 8;
  }
  return bytes;
}

double write_all(rocksdb::DB& db, const rocksdb::WriteOptions& options,
                 uint32_t rows, int writers) {
  Clock::time_point start;
  std::barrier ready(writers, [&]() noexcept { start = Clock::now(); });
  std::vector<Clock::time_point> finish_times(writers);
  std::vector<std::thread> threads;

  for (int worker = 0; worker < writers; ++worker) {
    threads.emplace_back([&, worker] {
      const std::string value(128, char(0x42));
      ready.arrive_and_wait();
      for (uint64_t i = 0; i < rows / writers; ++i) {
        const auto key = encode_key(((i * writers + worker) * 65537) % rows);
        check(db.Put(options, rocksdb::Slice(key.data(), key.size()), value));
      }
      finish_times[worker] = Clock::now();
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  const auto last_write =
      *std::max_element(finish_times.begin(), finish_times.end());
  return Seconds(last_write - start).count();
}

void read_all(rocksdb::DB& db, const rocksdb::ReadOptions& options,
              uint32_t rows) {
  for (uint64_t i = 0; i < rows; ++i) {
    const auto key = encode_key((i * 8191) % rows);
    rocksdb::PinnableSlice value;
    check(db.Get(options, db.DefaultColumnFamily(),
                 rocksdb::Slice(key.data(), key.size()), &value));
    if (value.size() != 128 || value[0] != char(0x42)) {
      std::exit(1);
    }
  }
}

int main(int argc, char** argv) {
  if (argc != 3 && argc != 4) {
    return 2;
  }
  const bool sync = argc == 4;
  if (sync && std::string(argv[3]) != "sync") {
    return 2;
  }
  const uint32_t rows = sync ? 8192 : 131072;
  const int writers = std::stoi(argv[2]);
  if (writers != 1 && writers != 8) {
    return 2;
  }

  rocksdb::BlockBasedTableOptions table;
  table.block_size = 4096;
  table.block_cache = rocksdb::HyperClockCacheOptions(64 * kMiB).MakeSharedCache();
  table.filter_policy.reset(rocksdb::NewBloomFilterPolicy(12, false));
  rocksdb::Options options;
  options.create_if_missing = true;
  options.error_if_exists = true;
  options.write_buffer_size = 64 * kMiB;
  options.compression = rocksdb::kNoCompression;
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table));
  std::unique_ptr<rocksdb::DB> db;
  check(rocksdb::DB::Open(options, argv[1], &db));

  rocksdb::WriteOptions write_options;
  write_options.disableWAL = false;
  write_options.sync = sync;
  const double write_seconds = write_all(*db, write_options, rows, writers);
  rocksdb::FlushOptions flush;
  flush.wait = true;
  check(db->Flush(flush));

  std::printf("sst_levels=");
  for (int level = 0; level < db->NumberLevels(); ++level) {
    const auto property = "rocksdb.num-files-at-level" + std::to_string(level);
    std::string count;
    if (!db->GetProperty(property, &count)) {
      std::exit(1);
    }
    std::printf("%s%s", level == 0 ? "" : ",", count.c_str());
  }
  std::printf(" ");

  rocksdb::ReadOptions read_options;
  read_all(*db, read_options, rows);
  const auto read_start = Clock::now();
  read_all(*db, read_options, rows);
  const double read_seconds = Seconds(Clock::now() - read_start).count();
  std::printf("writers=%d records=%u sync=%d write_s=%.9f read_s=%.9f\n",
              writers, rows, sync, write_seconds, read_seconds);
  check(db->Close());
}
