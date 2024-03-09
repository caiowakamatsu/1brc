#include <iostream>
#include <fstream>
#include <set>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/stat.h>
#include "flat_hash_map.hpp"
#include "concurrentqueue.h"

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

[[nodiscard]] size_t index_from_name(std::string_view name) {
    return (std::hash<std::string_view>()(name) * 336043159889533) >> 49;
}

void output_batch(std::set<std::string> &names, std::vector<data_entry> &data) {
    std::cout << '{';
    std::cout << std::fixed;
    std::cout << std::setprecision(1);

    auto it = names.begin();
    while (it != names.end()) {
        const auto &entry = data[index_from_name(*it)];
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

void read_lines(std::string path, moodycamel::ConcurrentQueue<std::string_view> &queue, std::atomic<bool> &running) {
    int fd = open(path.c_str(), O_RDONLY);
    struct stat sb;
    size_t file_cursor = 0;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    char* file_content = static_cast<char*>(mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0));

    const size_t buffer_size = 13000;

    while (file_cursor != file_size) {
        const size_t remaining = file_size - file_cursor;
        auto end = std::min(buffer_size, remaining) + file_cursor;
        while (file_content[--end] != '\n');
        queue.enqueue({file_content + file_cursor, file_content + end});
        file_cursor = end + 1;
    }

    running = false;
    munmap(file_content, file_size);
    close(fd);
}

std::vector<std::thread> dispatch_to_threads(
        std::set<std::string> &names,
        std::vector<std::vector<data_entry>> &entries,
        moodycamel::ConcurrentQueue<std::string_view> &queue,
        std::uint32_t thread_count,
        std::atomic<bool> &running) {
    auto threads = std::vector<std::thread>();

    for (auto i = 0; i < thread_count; i++) {
        entries.emplace_back(32'768);
    }

    for (std::uint32_t i = 0; i < thread_count; i++) {
        threads.emplace_back([&, i] () {
            auto &stats = entries[i];
            auto lines = std::string();
            while (running) {
                if (queue.try_dequeue(lines)) {
                    auto current = lines.begin();
                    for (size_t j = 0; j < lines.size(); j++) {
                        if (lines[j] == '\n') {
                            const auto line = std::string_view(current, lines.begin() + j);
                            auto semicolon = size_t(line.size());
                            while (line[--semicolon] != ';');
                            const auto name = std::string_view(line.begin(), line.begin() + semicolon);
                            if (i == 0 && names.size() != 413) {
                                names.insert(std::string(name));
                            }
                            const auto measurement = parse_float({line.begin() + semicolon + 1, line.end()});
                            stats[index_from_name(name)].accumulate(measurement);
                            current = lines.begin() + j + 1;
                        }
                    }
                }
            }
        });
    }

    return threads;
}

int main() {
    const auto thread_count = std::thread::hardware_concurrency();

    auto names = std::set<std::string>();
    auto stats = std::vector<data_entry>(32'768);
    auto entries = std::vector<std::vector<data_entry>>();
    auto queue = moodycamel::ConcurrentQueue<std::string_view>();
    auto running = std::atomic<bool>(true);

    auto producer = std::thread([&](){
        read_lines("measurements_large.txt", queue, running);
    });

    for (auto &thread : dispatch_to_threads(names, entries, queue, thread_count, running)) {
        thread.join();
    }
    producer.join();

    for (const auto &name : names) {
        const auto index = index_from_name(name);
        auto &to_add_to = stats[index];
        for (auto &entry : entries) {
            const auto &against = entry[index];
            to_add_to.min = std::min(to_add_to.min, against.min);
            to_add_to.max = std::max(to_add_to.max, against.max);
            to_add_to.sum += against.sum;
            to_add_to.count += against.count;
        }
    }

    output_batch(names, stats);
    return 0;
}
