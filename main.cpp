#include <iostream>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <unordered_map>

std::stringstream load_file(const std::filesystem::path &path) {
    auto file = std::ifstream(path);
    auto stream = std::stringstream();
    stream << file.rdbuf();
    return stream;
}

struct data_entry {
    float min = std::numeric_limits<float>::infinity();
    float max = -std::numeric_limits<float>::infinity();
    float mean = 0.0f;
    float count = 0.0f;
};

int main() {
    auto file = load_file("measurements.txt");

    auto data = std::map<std::string, data_entry>();
    for (std::string line; std::getline(file, line, '\n');) {
        auto semicolon = size_t(line.size());
        while (line[--semicolon] != ';');
        const auto name = std::string(line.begin(), line.begin() + semicolon);
        auto &entry = data[name];
        const auto measurement = std::stof(std::string(line.begin() + semicolon + 1, line.end()));
        entry.min = measurement < entry.min ? measurement : entry.min;
        entry.max = measurement > entry.max ? measurement : entry.max;
        entry.mean = ((entry.mean * entry.count) + measurement) / (entry.count + 1.0f);
        entry.count += 1.0f;
    }


    std::cout << '{';
    std::cout << std::fixed;
    std::cout << std::setprecision(1);
    auto it = data.begin();
    while (it != data.end()) {
        const auto &[name, entry] = *it;
        std::cout << name << '=' << entry.min << '/' << entry.mean << '/' << entry.max;
        if (++it != data.end()) {
            std::cout << ", ";
        }
    }
    std::cout << '}';

    return 0;
}
