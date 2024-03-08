#include <iostream>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>

std::stringstream load_file(const std::filesystem::path &path) {
    auto file = std::ifstream(path);
    auto stream = std::stringstream();
    stream << file.rdbuf();
    return stream;
}

struct reading {
    std::string name;
    float measurement;

    explicit reading(std::string_view line) {
        auto semicolon = size_t(line.size());
        while (line[--semicolon] != ';');
        name = std::string(line.begin(), line.begin() + semicolon);
        measurement = std::stof(std::string(line.begin() + semicolon + 1, line.end()));
    }
};

std::vector<reading> generate_readings(std::stringstream &data) {
    auto readings = std::vector<reading>();

    for (std::string line; std::getline(data, line, '\n');)
        readings.emplace_back(line);

    return readings;
}

struct reading_over_time {
    float min = std::numeric_limits<float>::infinity();
    float max = -std::numeric_limits<float>::infinity();
    float mean = 0.0f;
    float count = 0.0f;
};

int main() {
    auto file = load_file("measurements.txt");
    auto readings = generate_readings(file);
    auto data = std::unordered_map<std::string, reading_over_time>();
    for (const auto &reading : readings) {
        auto &over_time = data[reading.name];
        const auto measure = reading.measurement;
        over_time.min = measure < over_time.min ? measure : over_time.min;
        over_time.max = measure > over_time.max ? measure : over_time.max;
        over_time.mean = ((over_time.mean * over_time.count) + measure) / (over_time.count + 1.0f);
        over_time.count += 1.0f;
    }
    std::sort(readings.begin(), readings.end(), [&data](const reading &a, const reading &b){
       return a.name < b.name;
    });
    std::cout << '{';
    std::cout << std::fixed;
    std::cout << std::setprecision(1);
    for (size_t i = 0; i < readings.size(); i++) {
        const auto &reading = readings[i];
        auto &time = data[reading.name];
        if (time.count > 0.0f) {
            time.count = -1.0f;
            std::cout << reading.name << '=' << time.min << '/' << time.mean << '/' << time.max;
            if (i + 1 < readings.size()) {
                std::cout << ", ";
            }
        }
    }
    std::cout << '}';

    return 0;
}
