#include <iostream>
#include <fstream>
#include <set>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/stat.h>
#include <bitset>
#include <thread>
#include <arm_neon.h>



template <typename ValueT>
class string_view_map {
public:
    explicit string_view_map(std::uint32_t size = 70'000) : size(size), data(size), keys(size) {

    }

    [[nodiscard]] ValueT &operator[](std::string_view key) {
        auto hash = std::uint32_t(528442133u);
        for (char c : key) {
            hash ^= (c * (hash >> 4));
        }
        auto index = (hash % 65'536u) - 1;
        while (++index < size && !keys[index].empty() && (keys[index] != key));
        keys[index] = key;
        return data[index];
    }

private:
    size_t size = 0;
    std::vector<ValueT> data = {};
    std::vector<std::string_view> keys = {};

};

int parse_float(std::string_view input) {
    if (input[0] == '-') {
        if (input.length() == 5) {
            return -(((input[1] - '0') * 100) + ((input[2] - '0') * 10) + (input[4] - '0'));
        } else if (input.length() == 4) {
            return -(((input[1] - '0') * 10) + (input[3] - '0'));
        }
    } else {
        if (input.length() == 4) {
            return ((input[0] - '0') * 100) + ((input[1] - '0') * 10) + (input[3] - '0');
        } else if (input.length() == 3) {
            return ((input[0] - '0') * 10) + (input[2] - '0');
        }
    }
}

struct data_entry {
    int min = std::numeric_limits<int>::max();
    int max = -std::numeric_limits<int>::max();
    int sum = 0;
    int count = 0;

    void accumulate(int measurement) {
        min = std::min(min, measurement);
        max = std::max(max, measurement);
        sum += measurement;
        count += 1;
    }
};

void output_batch(std::set<std::string> &names, string_view_map<data_entry> &data) {
    std::cout << '{';
    std::cout << std::fixed;
    std::cout << std::setprecision(1);

    auto it = names.begin();
    while (it != names.end()) {
        const auto &entry = data[*it];
        std::cout << *it << '='
            << static_cast<float>(entry.min) * 0.1f << '/'
            << (static_cast<float>(entry.sum) * 0.1f) / static_cast<float>(entry.count)
            << '/'
            << static_cast<float>(entry.max) * 0.1f;
        if (++it != names.end()) {
            std::cout << ", ";
        }
    }
    std::cout << '}';
}

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
line_read_boundaries read_lines(std::string path, std::uint32_t thread_count) {
    int fd = open(path.c_str(), O_RDONLY);
    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    // the +64 is for padding just to be safe
    char* file_content = static_cast<char*>(mmap(NULL, file_size + 64, PROT_READ, MAP_PRIVATE, fd, 0));

    auto boundaries = line_read_boundaries();
    boundaries.fd = fd;
    boundaries.file_size = file_size;
    boundaries.memory = file_content;

    size_t file_cursor = 0;
    const auto rough_estimate = file_size / thread_count;
    for (size_t i = 0; i < thread_count - 1; i++) {
        auto index = file_cursor + rough_estimate;
        while (file_content[--index] != '\n');
        boundaries.boundaries.emplace_back(file_content + file_cursor, file_content + index);
        file_cursor = index + 1;
    }
    boundaries.boundaries.emplace_back(file_content + file_cursor, file_content + file_size);

    return boundaries;
}

[[nodiscard]] std::string_view::iterator find_next(std::string_view::iterator begin, char value) {
    const auto mask = vld1q_dup_u8(reinterpret_cast<uint8_t const *>(&value));
    auto count = std::uint64_t(0);
    while (true) {
        const auto loaded = vld1q_u8(reinterpret_cast<uint8_t const *>(begin + count * 16));
        const auto base = begin + count * 16;
        const auto xored = veorq_s8(loaded, mask);
        if (vminvq_u8(xored) == 0) {
            if (vgetq_lane_u8(xored, 0) == 0) return 0 + base;
            if (vgetq_lane_u8(xored, 1) == 0) return 1 + base;
            if (vgetq_lane_u8(xored, 2) == 0) return 2 + base;
            if (vgetq_lane_u8(xored, 3) == 0) return 3 + base;
            if (vgetq_lane_u8(xored, 4) == 0) return 4 + base;
            if (vgetq_lane_u8(xored, 5) == 0) return 5 + base;
            if (vgetq_lane_u8(xored, 6) == 0) return 6 + base;
            if (vgetq_lane_u8(xored, 7) == 0) return 7 + base;
            if (vgetq_lane_u8(xored, 8) == 0) return 8 + base;
            if (vgetq_lane_u8(xored, 9) == 0) return 9 + base;
            if (vgetq_lane_u8(xored, 10) == 0) return 10 + base;
            if (vgetq_lane_u8(xored, 11) == 0) return 11 + base;
            if (vgetq_lane_u8(xored, 12) == 0) return 12 + base;
            if (vgetq_lane_u8(xored, 13) == 0) return 13 + base;
            if (vgetq_lane_u8(xored, 14) == 0) return 14 + base;
            if (vgetq_lane_u8(xored, 15) == 0) return 15 + base;
        }
        count += 1;
    }
}

int32x4_t load_numbers(std::int32_t a, std::int32_t b, std::int32_t c, std::int32_t d) {
    const auto packed = std::array<std::int32_t, 4>({a, b, c, d});
    return vld1q_s32(packed.data());
}

void unload(int32x4_t value, std::int32_t *a, std::int32_t *b, std::int32_t *c, std::int32_t *d) {
    auto packed = std::array<std::int32_t, 4>();
    vst1q_s32(packed.data(), value);
    *a = packed[0];
    *b = packed[1];
    *c = packed[2];
    *d = packed[3];
}

void simd_accumulate(string_view_map<data_entry> &stats, const std::array<std::string_view, 4> &cities, const std::array<std::int32_t, 4> &m) {
    const auto one = std::int32_t(1);
    auto entries = std::array<data_entry*, 4>({&stats[cities[0]], &stats[cities[1]], &stats[cities[2]], &stats[cities[3]] });
    const auto measurements = load_numbers(m[0], m[1], m[2], m[3]);
    const auto minimums = load_numbers(entries[0]->min, entries[1]->min, entries[2]->min, entries[3]->min);
    const auto maximums = load_numbers(entries[0]->max, entries[1]->max, entries[2]->max, entries[3]->max);
    const auto sums = load_numbers(entries[0]->sum, entries[1]->sum, entries[2]->sum, entries[3]->sum);
    const auto counts = load_numbers(entries[0]->count, entries[1]->count, entries[2]->count, entries[3]->count);
    const auto minimums_op = vminq_s32(minimums, measurements);
    const auto maximums_op = vmaxq_s32(maximums, measurements);
    const auto sums_op = vaddq_s32(sums, measurements);
    const auto counts_op = vaddq_s32(counts, vld1q_dup_s32(&one));
    unload(minimums_op, &entries[0]->min, &entries[1]->min, &entries[2]->min, &entries[3]->min);
    unload(maximums_op, &entries[0]->max, &entries[1]->max, &entries[2]->max, &entries[3]->max);
    unload(sums_op, &entries[0]->sum, &entries[1]->sum, &entries[2]->sum, &entries[3]->sum);
    unload(counts_op, &entries[0]->count, &entries[1]->count, &entries[2]->count, &entries[3]->count);
}

std::vector<std::thread> dispatch_to_threads(
        std::set<std::string> &names,
        std::vector<string_view_map<data_entry>> &entries,
        line_read_boundaries &tasks,
        std::uint32_t thread_count) {
    auto threads = std::vector<std::thread>();

    for (auto i = 0; i < thread_count; i++) {
        entries.emplace_back();
    }

    for (std::uint32_t i = 0; i < thread_count; i++) {
        threads.emplace_back([&, i] () {
            auto &stats = entries[i];
            const auto lines = tasks.boundaries[i];

            auto current = lines.begin();

            const auto load_next = [i, &names, &current, &lines](std::string_view &city, std::int32_t &measurement) -> bool {
                const auto after_city = find_next(current, ';');
                const auto after_measurement = find_next(after_city + 1, '\n');
                city = {current, after_city};
                measurement = parse_float({after_city + 1, after_measurement});
                if (i == 0 && names.size() != 413) {
                    names.insert(std::string(city));
                }
                current = after_measurement + 1;
                return current < lines.end();
            };

            auto simd_ok = true;
            simd_exit:
            while (simd_ok) {
                const auto before_simd_read = current;
//                 SIMD loop, 4 loaded at the same time
                auto loaded_cities = std::array<std::string_view, 4>();
                auto loaded_measurements = std::array<std::int32_t, 4>();
                for (int j = 0; j < 4; j++) {
                    if (!load_next(loaded_cities[j], loaded_measurements[j])) {
                        current = before_simd_read;
                        simd_ok = false;
                        goto simd_exit;
                    }
                }
                simd_accumulate(stats, loaded_cities, loaded_measurements);
            }

            auto city = std::string_view();
            auto measurement = std::int32_t();
            while (load_next(city, measurement)) {
                stats[city].accumulate(measurement);
            }
        });
    }

    return threads;
}

// 11001100101110001100100011000100000000
// 11001100101110001100100011000100101101

std::vector<std::string> get_stations() {
    auto file = std::ifstream("data/stations.txt");
    auto line = std::string();
    auto stations = std::vector<std::string>();
    while (std::getline(file, line)) {
        stations.push_back(line);
    }
    return stations;
}

void station_processing_main() {
    const auto stations = get_stations();
    auto map = string_view_map<int>();
    auto other_map = std::unordered_map<std::string, int>();

    auto names = std::set<std::string>();
    for (const auto &station : stations) {
        auto value = rand();
        map[station] = value;
        other_map[station] = value;
        names.insert(station);
    }

    for (auto name : names) {
        const auto ours = map[name];
        const auto theirs = map[name];
        if (ours != theirs) {
            auto b = 5;
        }
    }

    auto a = 5;
}

int main() {
    const auto thread_count = std::thread::hardware_concurrency();

    auto names = std::set<std::string>();
    auto stats = string_view_map<data_entry>();
    auto entries = std::vector<string_view_map<data_entry>>();

    auto lines = read_lines("data/measurements_large.txt", thread_count);

    for (auto &thread : dispatch_to_threads(names, entries, lines, thread_count)) {
        thread.join();
    }

    for (const auto &name : names) {
        auto &to_add_to = stats[name];
        for (auto &entry: entries) {
            const auto &against = entry[name];
            to_add_to.min = std::min(to_add_to.min, against.min);
            to_add_to.max = std::max(to_add_to.max, against.max);
            to_add_to.sum += against.sum;
            to_add_to.count += against.count;
        }
    }

    output_batch(names, stats);
    return 0;
}
