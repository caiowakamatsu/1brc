#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <string_view>
#include <thread>
#include <vector>

#include <iomanip>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <immintrin.h>

struct hash_entry {
  std::int64_t sum = 0;
  std::uint32_t count = 0;
  std::int16_t min = std::numeric_limits<std::int16_t>::max();
  std::int16_t max = std::numeric_limits<std::int16_t>::min();

  void update(hash_entry &entry) {
    sum += entry.sum;
    count += entry.count;
    min = min < entry.min ? min : entry.min;
    max = max > entry.max ? max : entry.max;
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
        values[current_index].update(value);
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

/**
 * You thought floating point was safe from being SIMD'ed? nope
 * The thing to realize, if we dont account for the `-` at the
 * start of the -XX.X case, we only have 3 cases
 * XX.X
 * -X.X
 * ;X.X
 * Using some mask and blend, we turn it into
 * XX.X
 * 0X.X
 * Which just becomes
 * XX.X
 */
[[nodiscard]] std::array<std::int16_t, 8>
parse_floats(char const *const __restrict__ float_ends[8]) {
  alignas(32) int words[8];
  for (int i = 0; i < 8; i++) {
    // read 4 chars "XX.X" directly
    std::memcpy(&words[i], float_ends[i] - 4, 4);
  }
  constexpr auto hundreds_factor = std::uint64_t(100);
  constexpr auto tens_factor = std::uint64_t(10);
  constexpr auto bullshit_dot_factor = std::uint64_t(0);
  constexpr auto ones_factor = std::uint64_t(1);
  constexpr auto multipler_magic = (ones_factor << 48) |
                                   (bullshit_dot_factor << 32) |
                                   (tens_factor << 16) | (hundreds_factor << 0);
  const auto multipler = _mm256_set1_epi64x(multipler_magic);

  const auto raw_chars =
      _mm256_load_si256(reinterpret_cast<const __m256i *>(words));

  // Let's clean this up shall we?
  const auto semi_mask = _mm256_cmpeq_epi8(raw_chars, _mm256_set1_epi8(';'));
  const auto neg_mask = _mm256_cmpeq_epi8(raw_chars, _mm256_set1_epi8('-'));
  const auto fix_mask = _mm256_or_si256(semi_mask, neg_mask);

  // Everything is in XX.X (character now)
  const auto chars =
      _mm256_blendv_epi8(raw_chars, _mm256_set1_epi8('0'), fix_mask);
  // Bring everything to their actual numerical values
  const auto digits = _mm256_sub_epi8(chars, _mm256_set1_epi8('0'));

  // hi im avx and i dont give 8bit * 8bit for some god foresaken reason
  // Contains the lower 4
  const auto digits16_lo = _mm256_cvtepu8_epi16(_mm256_castsi256_si128(digits));
  // Contains the higher 4
  const auto digits16_hi =
      _mm256_cvtepu8_epi16(_mm256_extracti128_si256(digits, 1));

  const auto low_values = _mm256_mullo_epi16(digits16_lo, multipler);
  const auto high_values = _mm256_mullo_epi16(digits16_hi, multipler);

  // Partial sum between (lane[n] + lane[n + 1])
  const auto low_partial = _mm256_hadd_epi16(low_values, low_values);
  const auto final_low_partial = _mm256_hadd_epi16(low_partial, low_partial);
  const auto high_partial = _mm256_hadd_epi16(high_values, high_values);
  const auto final_high_partial = _mm256_hadd_epi16(high_partial, high_partial);

  // We're going to store both vectors here in the same array
  alignas(16) std::int16_t outputs_duplicated[32];
  _mm256_storeu_si256(reinterpret_cast<__m256i *>(outputs_duplicated),
                      final_low_partial);
  _mm256_storeu_si256(reinterpret_cast<__m256i *>(outputs_duplicated + 16),
                      final_high_partial);

  // Does a faster way to do this exist? Probably
  auto values = std::array{
      outputs_duplicated[0 + 0],  outputs_duplicated[0 + 1],
      outputs_duplicated[0 + 8],  outputs_duplicated[0 + 9],
      outputs_duplicated[16 + 0], outputs_duplicated[16 + 1],
      outputs_duplicated[16 + 8], outputs_duplicated[16 + 9],
  };
  auto negatives = std::array<std::int16_t, 8>();
  for (int i = 0; i < 8; i++) {
    negatives[i] =
        (float_ends[i][-5] == '-') || (float_ends[i][-4] == '-') ? -1 : 1;
    negatives[i] *= values[i];
  }

  return negatives;
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

[[nodiscard]] std::array<int, 8>
find_semicolon_indices_safe(const char *__restrict data) {
  static auto semicolon_mask = _mm256_set1_epi8(';');

  auto previous_semicolon_write_index = 0;
  auto current_semicolon_write_index = 0;
  auto semicolon_indices = std::array<std::int32_t, 8>();

  int i = 0;
  while (current_semicolon_write_index != 8) {
    if (*(data + i) == ';') {
      semicolon_indices[current_semicolon_write_index++] = i;
    }
    i += 1;
  }
  return semicolon_indices;
}

// you better PINKY PROMISE that data contains 8 semicolons or else shit
// WILL hit the fan
[[nodiscard]] std::array<int, 8>
find_semicolon_indices(const char *__restrict__ data) {
  static auto semicolon_mask = _mm256_set1_epi8(';');

  auto previous_semicolon_write_index = 0;
  auto current_semicolon_write_index = 0;
  auto semicolon_indices = std::array<std::int32_t, 8>();

  auto loop_iteration = 0;
  while (current_semicolon_write_index != 8) {
    const auto str_section = _mm256_loadu_si256(
        reinterpret_cast<const __m256i *>(data + loop_iteration * 32));
    const auto search_result = _mm256_cmpeq_epi8(str_section, semicolon_mask);
    auto search_mask =
        std::bit_cast<std::uint32_t>(_mm256_movemask_epi8(search_result));

    while (search_mask != 0 && current_semicolon_write_index != 8) {
      const auto first_bit = std::countr_zero(search_mask);

      // Write the index
      semicolon_indices[current_semicolon_write_index++] =
          first_bit + loop_iteration * 32;
      search_mask &= ~(1u << first_bit);
    }

    previous_semicolon_write_index = current_semicolon_write_index;
    loop_iteration += 1;
  }

  return semicolon_indices;
}

[[nodiscard]] std::array<const char *, 8>
get_float_ends(const char *__restrict__ data, const int *__restrict__ lengths) {
  auto result = std::array<const char *, 8>();

  auto offset = 0;
  for (int i = 0; i < 8; i++) {
    result[i] = data + offset + lengths[i];

    offset += lengths[i] + 1;
  }

  return result;
}

struct parsed_lines {
  std::array<city, 8> cities;
  std::array<hash_entry, 8> readings;
};
[[nodiscard]] parsed_lines parse_lines(const char *__restrict__ data,
                                       const int *__restrict__ line_lengths) {
  const auto semicolon_indices = find_semicolon_indices(data);

  auto cities = std::array<city, 8>();
  auto readings = std::array<hash_entry, 8>();

  const auto float_end_ptrs = get_float_ends(data, line_lengths);
  const auto temp_values = parse_floats(float_end_ptrs.data());

  auto line_start_offset = 0;
  for (int i = 0; i < 8; i++) {
    const auto line_start = data + line_start_offset;
    const auto semicolon = data + semicolon_indices[i];
    const auto line_end = line_start + line_lengths[i];

    cities[i] = {.name = {line_start, semicolon}};

    readings[i] = {
        .sum = temp_values[i],
        .count = 1,
        .min = static_cast<std::int16_t>(temp_values[i]),
        .max = static_cast<std::int16_t>(temp_values[i]),
    };

    line_start_offset +=
        line_lengths[i] + 1; // our line lengths don't account for the \n
  }

  return {
      .cities = cities,
      .readings = readings,
  };
}

[[nodiscard]] parsed_lines
parse_lines(const std::array<std::string_view, 8> &lines) {
  auto result = parsed_lines();

  auto lengths = std::array<int, 8>();
  for (int i = 0; i < 8; i++) {
    lengths[i] = static_cast<std::int32_t>(lines[i].size());
  }
  return parse_lines(lines[0].data(), lengths.data());
}

void process_chunk(std::string_view data, hash_map<city, hash_entry> &results) {
  auto line_lengths_size = 0;
  auto lines = std::array<std::string_view, 8>();

  auto line = std::string_view();
  while (get_next_line(data, line)) {
    // Submit the current line
    lines[line_lengths_size++] = line;

    // Are we full (if so; process everything)
    if (line_lengths_size == 8) {
      const auto parsed = parse_lines(lines);
      for (int i = 0; i < 8; i++) {
        results.update(parsed.cities[i], parsed.readings[i]);
      }
      line_lengths_size = 0;
    }

    // Increment to the next line to process
    const auto new_start = data.begin() + line.size() + 1;
    if (new_start >= data.end()) {
      break;
    }
    data = {new_start, data.end()};
  }

  // Deal with remaining data
  for (int i = 0; i < line_lengths_size; i++) {
    const auto parsed = parse_line(lines[i]);
    results.update(parsed.city, parsed.reading);
  }
}

void test_float_parse() {
  auto dup = std::array<const char *, 8>({
      "-0.1",
      "-1.2",
      "12.3",
      ";0.1",
      ";1.2",
      "-0.3",
      "-0.4",
      "-0.4",
  });

  for (auto &d : dup) {
    d += 5;
  }

  const auto results = parse_floats(dup.data());

  for (auto result : results) {
    std::cout << result << " ";
  }
  std::cout << std::endl;
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
      process_chunk(boundaries.boundaries[index], results[index]);
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
