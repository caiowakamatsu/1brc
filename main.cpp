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
#include <random>

constexpr auto MAX_HASH_VALUE = 2551458;
/* maximum key range = 2413318, duplicates = 0 */
/* This number generated ^^^ by gperf was completely wrong. Had to manually update it for it to work properly.
 * Why did it break? Who knows, gperf seems to have been released over 20 years ago. I don't doubt that it breaks
 * for some reason that no one will ever want to fix.
 */

[[nodiscard]] std::uint32_t hash(const char *str, size_t len)
{
    static unsigned int asso_values[] =
            {
                    2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381,
                    2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381,
                    2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381,
                    2413381, 2413381,  243092,   97591, 2413381, 2413381,   17969, 2413381, 2413381,       0,
                    186197,       0, 2413381,       0, 2413381,    1580,    6230,       0,       0,     265,
                    0,       0,       0,  161777, 2413381,       0, 2413381, 2413381, 2413381, 2413381,
                    0,       0,       0, 2413381, 2413381,   29365,    1535,    6180,  352559,  437034,
                    419514,  234107,  323364,  468164,   75948,   14815,  214287,       0,  404274,  425039,
                    51445,   40716,  339919,    4125,   93896,   58486,  456774,  483419,   64020,  433589,
                    306994,     540,      30,      15,     155,       0,      30,      10,    2515,  215542,
                    150102,     285,  121222,   58104,   68721,      15,   57790,   31905,     250,     165,
                    0,     585,    5975,      60,       5,     755,    1405,     100,      70,     710,
                    4345,      35,     500,  195837,   19435,   27910,    7015,    1955,  407554,  254537,
                    123116,   95211,   23670,   11815,   24675,  184542,  383964,   42505,  316839,  112962,
                    9495,    1415,  227513,   28657,     175,      70,       5,      10,      15,     385,
                    1950,       5,      25,     320,       0,      30,     250,      30,       5,      35,
                    0,      65,     255,    3075,      45,     190,      50,      75,     365,    1895,
                    275,   26350,    4000,    1000,      80,    2030,     655,    1020,     230,    1560,
                    9490,     415,    4280,     210,      25,     105,     125,      50,    1415,      35,
                    435,       0,     475,       0,     220,    9115,   31440,   15310,    6300,     150,
                    11526,    8160,       5,    7400,   40675,    2025,       5,       0,       0,      50,
                    0,    2650,   27995,     850,       0, 2413381,   10720,   77086,    3530,       0,
                    2413381, 2413381,       0, 2413381, 2413381,      15,       0,       0, 2413381,       0,
                    15, 2413381, 2413381,       5,      30, 2413381, 2413381, 2413381, 2413381, 2413381,
                    2413381,      15,       0, 2413381, 2413381, 2413381,       0,     295, 2413381, 2413381,
                    2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381,
                    2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381,
                    2413381, 2413381, 2413381, 2413381, 2413381, 2413381, 2413381
            };
    unsigned int hval = len;

    switch (hval)
    {
        default:
            hval += asso_values[(unsigned char)str[20]+1];
            /*FALLTHROUGH*/
        case 20:
        case 19:
        case 18:
        case 17:
            hval += asso_values[(unsigned char)str[16]];
            /*FALLTHROUGH*/
        case 16:
        case 15:
        case 14:
            hval += asso_values[(unsigned char)str[13]];
            /*FALLTHROUGH*/
        case 13:
        case 12:
        case 11:
            hval += asso_values[(unsigned char)str[10]+1];
            /*FALLTHROUGH*/
        case 10:
            hval += asso_values[(unsigned char)str[9]];
            /*FALLTHROUGH*/
        case 9:
            hval += asso_values[(unsigned char)str[8]];
            /*FALLTHROUGH*/
        case 8:
            hval += asso_values[(unsigned char)str[7]];
            /*FALLTHROUGH*/
        case 7:
            hval += asso_values[(unsigned char)str[6]+1];
            /*FALLTHROUGH*/
        case 6:
            hval += asso_values[(unsigned char)str[5]+8];
            /*FALLTHROUGH*/
        case 5:
            hval += asso_values[(unsigned char)str[4]+4];
            /*FALLTHROUGH*/
        case 4:
            hval += asso_values[(unsigned char)str[3]+21];
            /*FALLTHROUGH*/
        case 3:
            hval += asso_values[(unsigned char)str[2]];
            /*FALLTHROUGH*/
        case 2:
            hval += asso_values[(unsigned char)str[1]+16];
            /*FALLTHROUGH*/
        case 1:
            hval += asso_values[(unsigned char)str[0]];
            break;
    }
    return hval + asso_values[(unsigned char)str[len - 1]];
}

template <typename ValueT>
class string_view_map {
public:
    explicit string_view_map(std::uint32_t size = MAX_HASH_VALUE) : index_table(size, -1) {
        data.reserve(size / 1000); // good enough
    }

    [[nodiscard]] ValueT &operator[](std::string_view key) {
        const auto perfect_hash = hash(key.data(), key.size());
        const auto table_index = index_table[perfect_hash];
        if (table_index == -1) {
            index_table[perfect_hash] = data.size();
            return data.emplace_back();
        } else {
            return data[table_index];
        }
        return data[perfect_hash];
    }

private:
    std::vector<std::int32_t> index_table = {};
    std::vector<ValueT> data = {};
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
    const auto zero = std::uint8_t(0);
    const auto zero_vec = vld1q_dup_u8(&zero);
    auto count = std::uint64_t(0);
    while (true) {
        const auto loaded = vld1q_u8(reinterpret_cast<uint8_t const *>(begin + count * 16));
        const auto base = begin + count * 16;
        const auto xored = veorq_s8(loaded, mask);
        if (vminvq_u8(xored) == 0) {
            const auto compare_result = vceqq_u8(zero_vec, xored);
            const auto as_u64s = vreinterpretq_u64_u8(compare_result);
            auto unpacked = std::array<std::uint64_t, 2>();
            vst1q_u64(unpacked.data(), as_u64s);
            const auto first = (64 - __builtin_clzll(unpacked[0])) / 8;
            const auto second = (64 - __builtin_clzll(unpacked[1])) / 8;

            return ((first == 0 ? second + 8 : first) - 1) + base;
        }
        count += 1;
    }
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

    auto names = std::set<std::string>();
    for (const auto &station : stations) {
        names.insert(station);
    }

    auto rd = std::random_device();
    auto engine = std::mt19937(rd());
    auto distribution = std::uniform_int_distribution<uint32_t>();

    const auto hash_name = [](std::string_view name){
        auto hash = std::uint32_t(528442133u);
        for (char c : name) {
            hash ^= (c * (hash >> 4));
        }
        return hash;
    };

    while (true) {
        const auto number = distribution(engine);
        auto data = std::set<std::uint32_t>();
        for (const auto &name : names) {
            const auto hash = hash_name(name);
            data.insert((hash * number) >> 19);
        }

        if (data.size() == names.size()) {
            std::cout << "Found number that works: " << number << std::endl;
        }
    }
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
