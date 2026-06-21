#pragma once

#include "amm/bounded_queue.hpp"
#include "amm/ledger.hpp"
#include "amm/types.hpp"
#include <atomic>
#include <cstdint>
#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

namespace amm {

class Market {
public:
    explicit Market(SymbolConfig config);
    [[nodiscard]] Price current_price() const { return current_price_; }
    [[nodiscard]] const SymbolConfig& config() const { return config_; }
    [[nodiscard]] Quantity buy_quantity() const { return buy_quantity_; }
    [[nodiscard]] Quantity sell_quantity() const { return sell_quantity_; }
    [[nodiscard]] std::size_t pending_count() const { return buys_.size() + sells_.size(); }
    [[nodiscard]] bool eligible(const Order& order) const;
    [[nodiscard]] Price projected_price(OrderSide side, Quantity quantity) const;
    Price apply(OrderSide side, Quantity quantity);
    void pend(Order order);
    std::optional<Order> take_eligible(OrderSide side);
    std::vector<Order> take_all_pending();

private:
    struct BuyPriority {
        bool operator()(const Order& a, const Order& b) const;
    };
    struct SellPriority {
        bool operator()(const Order& a, const Order& b) const;
    };

    SymbolConfig config_;
    Price current_price_{};
    Quantity buy_quantity_{};
    Quantity sell_quantity_{};
    std::priority_queue<Order, std::vector<Order>, BuyPriority> buys_;
    std::priority_queue<Order, std::vector<Order>, SellPriority> sells_;
};

class Engine {
public:
    Engine(const SimulationConfig& config, BoundedMpscQueue<Order>& queue,
           std::atomic_bool& stop_requested, std::atomic_bool& fatal_error);
    RunMetrics run();

private:
    void process(Order order);
    void execute(Order order, bool market_order);
    void cascade(Market& market, Price old_price);
    void expire_pending();
    void verify_invariants() const;

    const SimulationConfig& config_;
    BoundedMpscQueue<Order>& queue_;
    std::atomic_bool& stop_requested_;
    std::atomic_bool& fatal_error_;
    std::unordered_map<std::string, Market> markets_;
    std::unique_ptr<Ledger> ledger_;
    RunMetrics metrics_;
    std::uint64_t next_execution_id_{1};
    std::uint64_t engine_sequence_{};
};

} // namespace amm

