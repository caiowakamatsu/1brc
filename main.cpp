#include <iostream>
#include <array>
#include <filesystem>
#include <fstream>
#include <ranges>
#include <sstream>
#include <map>

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
    float mean = 0.0f;
    float count = 0.0f;
};

void output_batch(std::map<std::string, data_entry> &data) {
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
}

void process_batch(std::span<std::string> lines, std::map<std::string, data_entry> &data) {
    for (const auto &line : lines) {
        auto semicolon = size_t(line.size());
        while (line[--semicolon] != ';');
        const auto name = std::string(line.begin(), line.begin() + semicolon);
        auto &entry = data[name];
        const auto measurement = parse_float({line.begin() + semicolon + 1, line.end()});
        entry.min = measurement < entry.min ? measurement : entry.min;
        entry.max = measurement > entry.max ? measurement : entry.max;
        entry.mean = ((entry.mean * entry.count) + measurement) / (entry.count + 1.0f);
        entry.count += 1.0f;
    }
}

template <size_t MaxBatchSize>
class buffered_batch_reader {
public:
    explicit buffered_batch_reader(const std::filesystem::path& path) : cursor(0) {
        auto file = std::ifstream(path);
        auto stream = std::stringstream();
        stream << file.rdbuf();
        buffer = stream.str();
    }

    struct batch_read_result {
        std::array<std::string, MaxBatchSize> lines = {};
        size_t count = 0;
    };
    [[nodiscard]] batch_read_result next_batch() {
        auto result = batch_read_result();

        while (result.count != MaxBatchSize && cursor < buffer.size()) {
            auto start = buffer.begin() + cursor;
            auto end = start;
            size_t count = 1;
            while (++count, *(++end) != '\n');
            result.lines[result.count++] = {start, end};
            cursor += count;
        }

        return result;
    }

private:
    std::string buffer;
    size_t cursor;
};

int main() {
    auto reader = buffered_batch_reader<4096>("measurements.txt");
    auto data = std::map<std::string, data_entry>();

    while (true) {
        auto batch_result = reader.next_batch();
        if (batch_result.count == 0) {
            break;
        }
        process_batch({batch_result.lines.begin(), batch_result.lines.begin() + batch_result.count}, data);
    }

    output_batch(data);

    return 0;
}
