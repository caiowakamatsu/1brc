#include <iostream>
#include <array>
#include <filesystem>
#include <fstream>
#include <ranges>
#include <sstream>
#include <set>
#include <sys/event.h>
#include <sys/fcntl.h>
#include <print>

#include "flat_hash_map.hpp"
#include "concurrentqueue.h"

int parse_float(std::string_view input) {
    if (input[0] == '-') {
        if (input.length() == 5) {
            // -XX.X
            return -(((input[1] - '0') * 100) + ((input[2] - '0') * 10) + (input[4] - '0'));
        } else if (input.length() == 4) {
            // -X.X
            return -(((input[1] - '0') * 10) + (input[3] - '0'));
        }
    } else {
        if (input.length() == 4) {
            // XX.X
            return ((input[0] - '0') * 100) + ((input[1] - '0') * 10) + (input[3] - '0');
        } else if (input.length() == 3) {
            // X.X
            return ((input[0] - '0') * 10) + (input[2] - '0');
        }
    }
}

struct data_entry {
    int min = std::numeric_limits<int>::max();
    int max = -std::numeric_limits<int>::max();
    int sum = 0;
    int count = 0.;

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

void read_lines(std::string path, moodycamel::ConcurrentQueue<std::string> &queue, std::atomic<bool> &running) {
    int fd = open(path.c_str(), O_RDONLY);
    int kq = kqueue();
    struct kevent event;
    EV_SET(&event, fd, EVFILT_READ, EV_ADD, 0, 0, NULL);
    kevent(kq, &event, 1, NULL, 0, NULL);

    auto back_buffer = std::string(8'192 * 4, '$');
    while (true) {
        int nevents = kevent(kq, NULL, 0, &event, 1, NULL);
        if (nevents == -1) {
            perror("kevent");
            exit(EXIT_FAILURE);
        }

        if (nevents > 0 && event.filter == EVFILT_READ) {
            const auto first_empty = back_buffer.find_first_of('$');
            ssize_t bytes_read = read(fd, back_buffer.data() + first_empty, back_buffer.size() - first_empty);
            if (bytes_read == -1) {
                perror("read");
                exit(EXIT_FAILURE);
            }
            if (bytes_read == 0) {
                break;
            }

            const auto last_new_line = back_buffer.find_last_of('\n') + 1;
            queue.enqueue(std::string(back_buffer.begin(), back_buffer.begin() + last_new_line));

            if (bytes_read != back_buffer.size() - first_empty) {
                break;
            }
            const auto remaining = std::string_view(back_buffer.begin() + last_new_line, back_buffer.end());
            std::memcpy(back_buffer.data(), remaining.data(), remaining.size());
            back_buffer[remaining.size()] = '$';
        } else {
            break;
        }
    }

    running = false;
    close(fd);
    close(kq);
}

std::vector<std::thread> dispatch_to_threads(
        std::set<std::string> &names,
        std::vector<std::vector<data_entry>> &entries,
        moodycamel::ConcurrentQueue<std::string> &queue,
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
    auto queue = moodycamel::ConcurrentQueue<std::string>();
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
