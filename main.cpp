#include <iostream>
#include <fstream>
#include <set>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/stat.h>
#include <bitset>
#include <thread>



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
    char* file_content = static_cast<char*>(mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0));

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

struct city_parse_result {
    std::int32_t hash = 0;
    std::string_view::iterator end = nullptr;
};
[[nodiscard]] city_parse_result parse_city(std::string_view::iterator begin, std::string_view::iterator end) {
    auto result = city_parse_result();
//    result.hash

    return result;
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

            for (size_t j = 0; j < lines.size(); j++) {
                if (lines[j] == '\n') {
                    auto line = std::string_view({current, lines.begin() + j});
                    auto semicolon = size_t(line.size());
                    while (line[--semicolon] != ';');
                    const auto name = std::string_view(line.begin(), line.begin() + semicolon);
                    if (i == 0 && names.size() != 413) {
                        names.insert(std::string(name));
                    }
                    const auto measurement = parse_float({line.begin() + semicolon + 1, line.end()});
                    stats[name].accumulate(measurement);
                    current = lines.begin() + j + 1;
                }
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
