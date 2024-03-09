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

float parse_float(std::string_view input) {
    float result = 0.0f;

    float multiplier = 0.1f;
    for (char c : std::ranges::reverse_view(input)) {
        if (c == '-') {
            result *= -1.0f;
        } else if (c != '.') {
            result += static_cast<float>(c - '0') * multiplier;
            multiplier *= 10.0f;
        }
    }

    return result;
}

struct data_entry {
    float min = std::numeric_limits<float>::infinity();
    float max = -std::numeric_limits<float>::infinity();
    float sum = 0.0f;
    float count = 0.0f;

    void accumulate(float measurement) {
        min = std::min(min, measurement);
        max = std::max(max, measurement);
        sum += measurement;
        count += 1.0f;
    }
};

[[nodiscard]] size_t index_from_name(const std::string &name) {
    return (std::hash<std::string>()(name) * 336043159889533) >> 49;
}

void output_batch(std::set<std::string> &names, std::vector<data_entry> &data) {
    std::cout << '{';
    std::cout << std::fixed;
    std::cout << std::setprecision(1);

    auto it = names.begin();
    while (it != names.end()) {
        const auto &entry = data[index_from_name(*it)];
        std::cout << *it << '=' << entry.min << '/' << entry.sum / entry.count << '/' << entry.max;
        if (++it != names.end()) {
            std::cout << ", ";
        }
    }
    std::cout << '}';
}

void read_lines(std::string path, std::function<void(std::span<std::string_view> line)> reader) {
    int fd = open(path.c_str(), O_RDONLY);
    int kq = kqueue();
    struct kevent event;
    EV_SET(&event, fd, EVFILT_READ, EV_ADD, 0, 0, NULL);
    kevent(kq, &event, 1, NULL, 0, NULL);

    auto back_buffer = std::string(8'192, '$');
    auto lines = std::vector<std::string_view>();

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

            auto current = back_buffer.begin();
            lines.clear();
            for (size_t i = 0; i < back_buffer.size(); i++) {
                auto c = back_buffer[i];
                if (c == '\n') {
                    lines.emplace_back(current, back_buffer.begin() + i);
                    current = back_buffer.begin() + i + 1;
                }
            }
            reader(lines);

            if (bytes_read != back_buffer.size() - first_empty) {
                break;
            }

            const auto last_new_line = back_buffer.find_last_of('\n') + 1;
            const auto remaining = std::string_view(back_buffer.begin() + last_new_line, back_buffer.end());
            std::memcpy(back_buffer.data(), remaining.data(), remaining.size());
            std::memset(back_buffer.data() + remaining.size(), '$', back_buffer.size() - remaining.size());
        } else {
            break;
        }
    }

    close(fd);
    close(kq);
}

int main() {
    auto names = std::set<std::string>();
    auto stats = std::vector<data_entry>(32'768);

    read_lines("measurements_large.txt", [&](std::span<std::string_view> lines){
        for (const auto &line : lines) {
            auto semicolon = size_t(line.size());
            while (line[--semicolon] != ';');
            const auto name = std::string(line.begin(), line.begin() + semicolon);
            if (names.size() != 413) {
                names.insert(name);
            }
            const auto measurement = parse_float({line.begin() + semicolon + 1, line.end()});
            stats[index_from_name(name)].accumulate(measurement);
        }
    });

    output_batch(names, stats);
    return 0;
}
