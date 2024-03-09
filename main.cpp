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

void read_lines(std::string path, std::function<void(std::string_view line)> reader) {
    auto file = std::ifstream(path, std::ios::ate);
    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);

    auto buffer = std::string(size, ' ');
    file.read(buffer.data(), size);

    auto lines = std::vector<std::string>();
    auto current = std::string();
    for (auto c : buffer) {
        if (c == '\n') {
            reader(current);
            lines.push_back(current);
            current = "";
        } else {
            current.push_back(c);
        }
    }
}

int main() {
    auto names = std::set<std::string>();
    auto stats = std::vector<data_entry>(32'768);

    read_lines("measurements.txt", [&](std::string_view line){
        auto semicolon = size_t(line.size());
        while (line[--semicolon] != ';');
        const auto name = std::string(line.begin(), line.begin() + semicolon);
        if (names.size() != 413) {
            names.insert(name);
        }
        const auto measurement = parse_float({line.begin() + semicolon + 1, line.end()});
        stats[index_from_name(name)].accumulate(measurement);
    });

    output_batch(names, stats);
    return 0;
}
