#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <string_view>
#include <thread>
#include <vector>

#include <iomanip>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

struct hash_entry {
  std::int64_t sum = 0;
  std::uint32_t count = 0;
  std::int16_t min = std::numeric_limits<std::int16_t>::max();
  std::int16_t max = std::numeric_limits<std::int16_t>::min();

  hash_entry operator+=(hash_entry entry) {
    sum += entry.sum;
    count += entry.count;
    min = min < entry.min ? min : entry.min;
    max = max > entry.max ? max : entry.max;
    return *this;
  }
};

struct city {
  std::string_view name = {};

  [[nodiscard]] bool operator==(const city &other) const noexcept {
    return other.name == this->name;
  }

  [[nodiscard]] std::uint32_t hash() const noexcept {
    auto hash_value = std::uint32_t();

    const auto size = name.size() < 4 ? name.size() : 4;
    std::memcpy(&hash_value, name.data(), size);

    return hash_value;
  }
};

// KeyT and ValueT should be trivially copyable
template <typename KeyT, typename ValueT> class hash_map {
private:
  std::unique_ptr<ValueT[]> values;
  std::unique_ptr<KeyT[]> keys;
  std::uint64_t size;

public:
  hash_map(std::uint64_t size)
      : values(std::make_unique<ValueT[]>(size)),
        keys(std::make_unique<KeyT[]>(size)), size(size) {}

  hash_map(hash_map<KeyT, ValueT> &&other) noexcept
      : values(std::move(other.values)), keys(std::move(other.keys)),
        size(other.size) {}

  hash_map(const hash_map &other) = delete;

  hash_map &operator=(hash_map &&other) = default;

  // hash_map &operator==

  void update(KeyT key, ValueT value) {
    const auto hash = key.hash();

    const auto initial_index = hash % size;
    auto current_index = initial_index;
    do {
      if (keys[current_index] == key) {
        // Found the entry
        keys[current_index] = key;
        values[current_index] += value;
        break;
      } else if (keys[current_index] == KeyT()) {
        keys[current_index] = key;
        // Found an open entry, write
        values[current_index] = value;
        break;
      }
      // Entry wasn't interesting, continue going forward

      // Automatic loop back
      current_index = (current_index + 1) % size;
    } while (current_index != initial_index);
  }

  void merge(const hash_map<KeyT, ValueT> &other) {
    for (std::uint64_t i = 0; i < other.size; i++) {
      if (other.keys[i] != KeyT()) {
        update(other.keys[i], other.values[i]);
      }
    }
  }

  void print() const noexcept {
    std::cout << '{';
    std::cout << std::fixed;
    std::cout << std::setprecision(1);
    auto valid = 0;
    for (std::uint64_t i = 0; i < size; i++) {
      if (keys[i] != KeyT()) {
        if (valid++ != 0) {
          std::cout << ",";
        }
        const auto &entry = values[i];
        std::cout << keys[i].name << '=' << static_cast<float>(entry.min) * 0.1f
                  << '/'
                  << (static_cast<float>(entry.sum) * 0.1f) /
                         static_cast<float>(entry.count)
                  << '/' << static_cast<float>(entry.max) * 0.1f;
      }
    }
    std::cout << '}';
  }
};

struct line_read_boundaries {
  int fd = 0;
  size_t file_size = 0;
  char *memory = nullptr;
  std::vector<std::string_view> boundaries = {};

  ~line_read_boundaries() {
    munmap(memory, file_size);
    close(fd);
  }
};
/**
 * Sets up all of the required resources for threads to do their work
 * afterwards
 * @param path Path to measurements file
 * @param thread_count Amount of threads that will aggregate the data
 * @return Boundaries and resources required for doing work
 */
line_read_boundaries read_lines(std::string path, std::uint32_t thread_count) {
  int fd = open(path.c_str(), O_RDONLY);
  struct stat sb;
  fstat(fd, &sb);
  size_t file_size = sb.st_size;
  // the +64 is for padding just to be safe
  char *file_content = static_cast<char *>(
      mmap(NULL, file_size + 64, PROT_READ, MAP_PRIVATE, fd, 0));

  auto boundaries = line_read_boundaries();
  boundaries.fd = fd;
  boundaries.file_size = file_size;
  boundaries.memory = file_content;

  size_t file_cursor = 0;
  const auto rough_estimate = file_size / thread_count;
  for (size_t i = 0; i < thread_count - 1; i++) {
    auto index = file_cursor + rough_estimate;
    while (file_content[--index] != '\n')
      ;
    boundaries.boundaries.emplace_back(file_content + file_cursor,
                                       file_content + index);
    file_cursor = index + 1;
  }
  boundaries.boundaries.emplace_back(file_content + file_cursor,
                                     file_content + file_size);

  return boundaries;
}

[[nodiscard]] bool get_next_line(std::string_view &source,
                                 std::string_view &line) {
  if (source.empty()) {
    return false;
  }
  if (const auto newline = source.find('\n');
      newline != std::string_view::npos) {
    line = source.substr(0, newline);
    return true;
  }

  return false;
}

struct worker {
  // This is REQUIRED have \n as the last character
  std::byte *data = nullptr;

  void run() {}
};

/**
 * Takes a string_view of a float in one of the following forms
 * 1) -XX.X
 * 2)  XX.X
 * 3)  -X.X
 * 4)   X.X
 * and gives back the fixed point representation of it
 * @param input The float as a string
 * @return std::int32_t fixed point representation of the float (* 0.1f to get
 * original value)
 */
[[nodiscard]] int parse_float(std::string_view input) {
  const auto neg = input[0] == '-';
  const auto ones = input[input.size() - 1] - '0';
  const auto tens = input[input.size() - 3] - '0';
  auto hundreds = 0;
  if ((input.size() == 5 && neg) || (input.size() == 4 && !neg)) {
    hundreds = input[static_cast<int>(neg)] - '0';
  }

  return (neg ? -1 : 1) * ((ones) + (tens * 10) + (hundreds * 100));
}

struct parsed_line {
  struct city city;
  hash_entry reading;
};
[[nodiscard]] parsed_line parse_line(std::string_view line) {
  const auto semicolon = line.find(';');

  const auto city_name =
      std::string_view(line.begin(), line.begin() + semicolon);
  const auto reading =
      std::string_view(line.begin() + semicolon + 1, line.end());

  const auto temp = parse_float(reading);

  return {.city =
              {
                  .name = city_name,
              },
          .reading = {.sum = temp,
                      .count = 1,
                      .min = static_cast<std::int16_t>(temp),
                      .max = static_cast<std::int16_t>(temp)}};
}

int main(int argc, char **argv) {
  if (argc != 2) {
    std::cerr << "usage: " << argv[0] << " <file>" << std::endl;
    return 1;
  }

  constexpr auto thread_count = 8;

  const auto boundaries = read_lines(argv[1], thread_count);

  auto results = std::vector<hash_map<city, hash_entry>>();
  for (int i = 0; i < thread_count; i++) {
    results.emplace_back(10000);
  }

  auto threads = std::vector<std::thread>();
  for (int i = 0; i < thread_count; i++) {
    threads.emplace_back([&boundaries, &results, index = i]() {
      auto data = boundaries.boundaries[index];
      auto line = std::string_view();
      while (get_next_line(data, line)) {
        const auto parsed = parse_line(line);
        results[index].update(parsed.city, parsed.reading);
        const auto new_start = data.begin() + line.size() + 1;
        if (new_start >= data.end()) {
          break;
        }
        data = {new_start, data.end()};
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  auto data = hash_map<city, hash_entry>(10000);
  for (const auto &result : results) {
    data.merge(result);
  }

  data.print();

  return 0;
}
