#include "amm/config.hpp"

#include <cmath>
#include <fstream>
#include <iomanip>
#include <limits>
#include <nlohmann/json.hpp>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string_view>

namespace amm {
namespace {
using json = nlohmann::json;

template <class T> void read_if(const json& j, const char* key, T& value) {
    if (j.contains(key)) value = j.at(key).get<T>();
}

void reject_unknown(const json& j, std::initializer_list<std::string_view> allowed, std::string_view where) {
    for (const auto& [key, ignored] : j.items()) {
        (void)ignored;
        bool known = false;
        for (const auto candidate : allowed) known = known || key == candidate;
        if (!known) throw std::invalid_argument("unknown configuration key '" + key + "' in " + std::string(where));
    }
}

void apply_json(SimulationConfig& config, const json& root) {
    reject_unknown(root, {"traders", "orders_per_trader", "queue_capacity", "seed", "workload",
                          "database", "commit_batch_size", "symbols"}, "root");
    read_if(root, "traders", config.traders);
    read_if(root, "orders_per_trader", config.orders_per_trader);
    read_if(root, "queue_capacity", config.queue_capacity);
    read_if(root, "seed", config.seed);
    read_if(root, "database", config.database_path);
    read_if(root, "commit_batch_size", config.commit_batch_size);

    if (root.contains("workload")) {
        const auto& w = root.at("workload");
        reject_unknown(w, {"market_order_probability", "buy_probability", "min_quantity",
                           "max_quantity", "limit_price_band"}, "workload");
        read_if(w, "market_order_probability", config.market_order_probability);
        read_if(w, "buy_probability", config.buy_probability);
        read_if(w, "min_quantity", config.min_quantity);
        read_if(w, "max_quantity", config.max_quantity);
        read_if(w, "limit_price_band", config.limit_price_band);
    }

    if (root.contains("symbols")) {
        config.symbols.clear();
        for (const auto& item : root.at("symbols")) {
            reject_unknown(item, {"ticker", "intrinsic_price", "liquidity", "max_deviation", "price_scale"}, "symbols[]");
            SymbolConfig symbol;
            read_if(item, "ticker", symbol.ticker);
            read_if(item, "liquidity", symbol.liquidity);
            read_if(item, "max_deviation", symbol.max_deviation);
            read_if(item, "price_scale", symbol.price_scale);
            if (item.contains("intrinsic_price")) {
                symbol.intrinsic_price = parse_price(item.at("intrinsic_price").get<double>(), symbol.price_scale);
            } else {
                symbol.intrinsic_price = parse_price(100.0, symbol.price_scale);
            }
            config.symbols.push_back(symbol);
        }
    }
}

std::uint64_t unsigned_arg(const std::string& name, const std::string& value) {
    if (value.empty() || value.front() == '-') throw std::invalid_argument("invalid value for " + name + ": " + value);
    std::size_t used{};
    const auto result = std::stoull(value, &used);
    if (used != value.size()) throw std::invalid_argument("invalid value for " + name + ": " + value);
    return result;
}

std::uint32_t uint32_arg(const std::string& name, const std::string& value) {
    const auto result = unsigned_arg(name, value);
    if (result > std::numeric_limits<std::uint32_t>::max()) throw std::out_of_range("value for " + name + " is too large");
    return static_cast<std::uint32_t>(result);
}

} // namespace

Price parse_price(double value, std::uint32_t scale) {
    if (!std::isfinite(value) || value <= 0.0 || scale == 0) throw std::invalid_argument("price must be finite and positive");
    const long double scaled = static_cast<long double>(value) * scale;
    if (scaled > static_cast<long double>(std::numeric_limits<Price>::max())) throw std::out_of_range("price is out of range");
    return static_cast<Price>(std::llround(scaled));
}

double display_price(Price value, std::uint32_t scale) {
    return static_cast<double>(value) / static_cast<double>(scale);
}

void validate(const SimulationConfig& c) {
    if (c.traders == 0 || c.orders_per_trader == 0) throw std::invalid_argument("traders and orders-per-trader must be positive");
    if (c.orders_per_trader == std::numeric_limits<std::uint32_t>::max())
        throw std::invalid_argument("orders-per-trader exceeds the supported producer sequence range");
    if (c.queue_capacity == 0) throw std::invalid_argument("queue capacity must be positive");
    if (c.commit_batch_size == 0) throw std::invalid_argument("commit batch size must be positive");
    if (c.database_path.empty()) throw std::invalid_argument("database path cannot be empty");
    if (c.market_order_probability < 0 || c.market_order_probability > 1 || c.buy_probability < 0 || c.buy_probability > 1)
        throw std::invalid_argument("probabilities must be between zero and one");
    if (c.min_quantity == 0 || c.max_quantity < c.min_quantity) throw std::invalid_argument("invalid quantity range");
    if (c.traders > static_cast<std::uint32_t>(std::numeric_limits<std::int32_t>::max()) ||
        c.max_quantity > static_cast<Quantity>(std::numeric_limits<std::int64_t>::max()))
        throw std::invalid_argument("trader count or quantity exceeds SQLite's signed integer range");
    if (!std::isfinite(c.limit_price_band) || c.limit_price_band <= 0 || c.limit_price_band >= 1)
        throw std::invalid_argument("limit price band must be between zero and one");
    if (c.symbols.empty()) throw std::invalid_argument("at least one symbol is required");

    std::set<std::string> tickers;
    for (const auto& s : c.symbols) {
        if (s.ticker.empty() || !tickers.insert(s.ticker).second) throw std::invalid_argument("symbol tickers must be non-empty and unique");
        if (s.intrinsic_price <= 0 || s.liquidity == 0 || s.price_scale == 0) throw std::invalid_argument("invalid market parameters for " + s.ticker);
        if (!std::isfinite(s.max_deviation) || s.max_deviation <= 0 || s.max_deviation >= 1)
            throw std::invalid_argument("max deviation must be between zero and one for " + s.ticker);
    }
}

CliResult load_configuration(int argc, char** argv) {
    SimulationConfig config;
    std::string config_path;
    for (int i = 1; i < argc; ++i) {
        if (std::string_view(argv[i]) == "--config") {
            if (++i == argc) throw std::invalid_argument("--config requires a path");
            config_path = argv[i];
        }
    }
    if (!config_path.empty()) {
        std::ifstream input(config_path);
        if (!input) throw std::runtime_error("cannot open configuration file: " + config_path);
        json root;
        input >> root;
        if (!root.is_object()) throw std::invalid_argument("configuration root must be an object");
        apply_json(config, root);
    }

    CliResult result{config, false};
    auto next = [&](int& i, const std::string& option) -> std::string {
        if (++i == argc) throw std::invalid_argument(option + " requires a value");
        return argv[i];
    };
    for (int i = 1; i < argc; ++i) {
        const std::string option = argv[i];
        if (option == "--help" || option == "-h") result.show_help = true;
        else if (option == "--config") ++i;
        else if (option == "--traders") result.config.traders = uint32_arg(option, next(i, option));
        else if (option == "--orders-per-trader") result.config.orders_per_trader = uint32_arg(option, next(i, option));
        else if (option == "--queue-capacity") result.config.queue_capacity = static_cast<std::size_t>(unsigned_arg(option, next(i, option)));
        else if (option == "--seed") result.config.seed = unsigned_arg(option, next(i, option));
        else if (option == "--database") result.config.database_path = next(i, option);
        else if (option == "--commit-batch-size") result.config.commit_batch_size = uint32_arg(option, next(i, option));
        else if (option == "--market-probability") result.config.market_order_probability = std::stod(next(i, option));
        else if (option == "--buy-probability") result.config.buy_probability = std::stod(next(i, option));
        else if (option == "--min-quantity") result.config.min_quantity = unsigned_arg(option, next(i, option));
        else if (option == "--max-quantity") result.config.max_quantity = unsigned_arg(option, next(i, option));
        else if (option == "--limit-band") result.config.limit_price_band = std::stod(next(i, option));
        else throw std::invalid_argument("unknown option: " + option);
    }
    if (!result.show_help) validate(result.config);
    return result;
}

std::string effective_config_json(const SimulationConfig& c) {
    json j{{"traders", c.traders}, {"orders_per_trader", c.orders_per_trader}, {"queue_capacity", c.queue_capacity},
           {"seed", c.seed}, {"database", c.database_path}, {"commit_batch_size", c.commit_batch_size},
           {"workload", {{"market_order_probability", c.market_order_probability}, {"buy_probability", c.buy_probability},
                         {"min_quantity", c.min_quantity}, {"max_quantity", c.max_quantity}, {"limit_price_band", c.limit_price_band}}}};
    j["symbols"] = json::array();
    for (const auto& s : c.symbols) j["symbols"].push_back({{"ticker", s.ticker}, {"intrinsic_price", display_price(s.intrinsic_price, s.price_scale)},
        {"liquidity", s.liquidity}, {"max_deviation", s.max_deviation}, {"price_scale", s.price_scale}});
    return j.dump();
}

void print_help(std::ostream& out) {
    out << "AMM trading simulator\n\n"
        << "Usage: amm-sim [options]\n"
        << "  --config PATH              JSON scenario (CLI options override it)\n"
        << "  --traders N                Producer thread count\n"
        << "  --orders-per-trader N      Finite workload per producer\n"
        << "  --queue-capacity N         Bounded MPSC capacity\n"
        << "  --seed N                   Global workload seed\n"
        << "  --database PATH            SQLite ledger path\n"
        << "  --commit-batch-size N      Events per transaction\n"
        << "  --market-probability P     Probability in [0,1]\n"
        << "  --buy-probability P        Probability in [0,1]\n"
        << "  --min-quantity N           Minimum generated quantity\n"
        << "  --max-quantity N           Maximum generated quantity\n"
        << "  --limit-band P             Limit offset fraction in (0,1)\n"
        << "  -h, --help                 Show this help\n";
}

} // namespace amm
