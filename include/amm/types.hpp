#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace amm {

using Price = std::int64_t;
using Quantity = std::uint64_t;

enum class OrderSide { Buy, Sell };
enum class OrderType { Market, Limit };
enum class OrderStatus { Accepted, Pending, Filled, Expired };

inline const char* to_string(OrderSide side) { return side == OrderSide::Buy ? "BUY" : "SELL"; }
inline const char* to_string(OrderType type) { return type == OrderType::Market ? "MARKET" : "LIMIT"; }
inline const char* to_string(OrderStatus status) {
    switch (status) {
    case OrderStatus::Accepted: return "ACCEPTED";
    case OrderStatus::Pending: return "PENDING";
    case OrderStatus::Filled: return "FILLED";
    case OrderStatus::Expired: return "EXPIRED";
    }
    return "UNKNOWN";
}

struct Order {
    std::uint64_t id{};
    std::uint32_t producer_id{};
    std::uint32_t producer_sequence{};
    std::uint64_t enqueue_sequence{};
    std::string symbol;
    OrderSide side{OrderSide::Buy};
    OrderType type{OrderType::Market};
    Quantity quantity{};
    std::optional<Price> limit_price;
};

struct Execution {
    std::uint64_t id{};
    std::uint64_t order_id{};
    std::string symbol;
    OrderSide side{OrderSide::Buy};
    Quantity quantity{};
    Price price{};
    std::uint64_t engine_sequence{};
};

struct SymbolConfig {
    std::string ticker{"SIM"};
    Price intrinsic_price{10'000};
    Quantity liquidity{100'000};
    double max_deviation{0.5};
    std::uint32_t price_scale{100};
};

struct SimulationConfig {
    std::uint32_t traders{8};
    std::uint32_t orders_per_trader{10'000};
    std::size_t queue_capacity{65'536};
    std::uint64_t seed{42};
    double market_order_probability{0.70};
    double buy_probability{0.50};
    Quantity min_quantity{1};
    Quantity max_quantity{100};
    double limit_price_band{0.10};
    std::string database_path{"amm-trades.db"};
    std::uint32_t commit_batch_size{1'000};
    std::vector<SymbolConfig> symbols{{}};
};

struct SymbolMetrics {
    std::string symbol;
    Price final_price{};
    std::uint32_t price_scale{100};
    Quantity buy_quantity{};
    Quantity sell_quantity{};
};

struct RunMetrics {
    std::uint64_t generated{};
    std::uint64_t accepted{};
    std::uint64_t processed{};
    std::uint64_t filled{};
    std::uint64_t expired{};
    std::uint64_t market_orders{};
    std::uint64_t limit_orders{};
    std::uint64_t executions{};
    std::uint64_t transactions{};
    std::size_t queue_high_water{};
    std::uint64_t blocked_pushes{};
    double elapsed_seconds{};
    bool interrupted{};
    bool failed{};
    std::string error;
    std::vector<SymbolMetrics> symbols;
};

} // namespace amm

