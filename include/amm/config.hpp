#pragma once

#include "amm/types.hpp"
#include <iosfwd>
#include <string>

namespace amm {

struct CliResult {
    SimulationConfig config;
    bool show_help{};
};

CliResult load_configuration(int argc, char** argv);
void validate(const SimulationConfig& config);
void print_help(std::ostream& out);
std::string effective_config_json(const SimulationConfig& config);
Price parse_price(double value, std::uint32_t scale);
double display_price(Price value, std::uint32_t scale);

} // namespace amm

