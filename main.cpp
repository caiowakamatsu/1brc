#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <string_view>
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

  [[nodiscard]] hash_map<KeyT, ValueT>
  merge(const hash_map<KeyT, ValueT> other) const {
    auto merged = hash_map(*this);
    for (std::uint64_t i = 0; i < other.size; i++) {
      if (other.keys[i] != KeyT()) {
        merged.update(other.keys[i], other.values[i]);
      }
    }
    return merged;
  }

  hash_map(const hash_map<KeyT, ValueT> &other)
      : values(std::make_unique<ValueT[]>(size)),
        keys(std::make_unique<KeyT[]>(size)), size(size) {
    std::memcpy(values.get(), other.values.get(), sizeof(ValueT) * size);
    std::memcpy(keys.get(), other.keys.get(), sizeof(keys) * size);
  }

  void print() const noexcept {
    std::cout << '{';
    std::cout << std::fixed;
    std::cout << std::setprecision(1);
    for (std::uint64_t i = 0; i < size; i++) {
      if (keys[i] != KeyT()) {
        const auto &entry = values[i];
        std::cout << keys[i].name << '=' << static_cast<float>(entry.min) * 0.1f
                  << '/'
                  << (static_cast<float>(entry.sum) * 0.1f) /
                         static_cast<float>(entry.count)
                  << '/' << static_cast<float>(entry.max) * 0.1f;
        if (i + 1 != size) {
          std::cout << ',';
        }
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
 * Sets up all of the required resources for threads to do their work afterwards
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

[[nodiscard]] bool get_next_line(char *source, char *end,
                                 std::string_view &line) {
  if (source >= end) {
    return false;
  }
  auto total = std::string_view(source, end);
  if (const auto newline = total.find('\n');
      newline != std::string_view::npos) {
    line = {source, source + newline};
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
  if (input[0] == '-') {
    if (input.length() == 5) {
      return -(((input[1] - '0') * 100) + ((input[2] - '0') * 10) +
               (input[4] - '0'));
    } else if (input.length() == 4) {
      return -(((input[1] - '0') * 10) + (input[3] - '0'));
    }
  } else {
    if (input.length() == 4) {
      return ((input[0] - '0') * 100) + ((input[1] - '0') * 10) +
             (input[3] - '0');
    } else if (input.length() == 3) {
      return ((input[0] - '0') * 10) + (input[2] - '0');
    }
  }
  return 0;
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

  auto data = hash_map<city, hash_entry>(100'000);

  const auto boundaries = read_lines(argv[1], 1);
  auto line = std::string_view();
  auto source = boundaries.memory;
  while (
      get_next_line(source, boundaries.memory + boundaries.file_size, line)) {
    const auto parsed = parse_line(line);
    data.update(parsed.city, parsed.reading);
    source += line.size() + 1;
  }

  data.print();

  return 0;
}
