#include "amm/engine.hpp"

#include "amm/config.hpp"
#include "amm/ledger.hpp"
#include <cmath>
#include <limits>
#include <stdexcept>
#include <utility>

namespace amm {

Market::Market(SymbolConfig config) : config_(std::move(config)), current_price_(config_.intrinsic_price) {}

bool Market::BuyPriority::operator()(const Order& a, const Order& b) const {
    if (a.limit_price != b.limit_price) return a.limit_price < b.limit_price;
    return a.enqueue_sequence > b.enqueue_sequence;
}

bool Market::SellPriority::operator()(const Order& a, const Order& b) const {
    if (a.limit_price != b.limit_price) return a.limit_price > b.limit_price;
    return a.enqueue_sequence > b.enqueue_sequence;
}

bool Market::eligible(const Order& order) const {
    if (!order.limit_price) return true;
    return order.side == OrderSide::Buy ? current_price_ <= *order.limit_price : current_price_ >= *order.limit_price;
}

Price Market::projected_price(OrderSide side, Quantity quantity) const {
    Quantity buys = buy_quantity_;
    Quantity sells = sell_quantity_;
    if (side == OrderSide::Buy) {
        if (quantity > std::numeric_limits<Quantity>::max() - buys) throw std::overflow_error("buy volume overflow for " + config_.ticker);
        buys += quantity;
    } else {
        if (quantity > std::numeric_limits<Quantity>::max() - sells) throw std::overflow_error("sell volume overflow for " + config_.ticker);
        sells += quantity;
    }
    const long double net = static_cast<long double>(buys) - static_cast<long double>(sells);
    const long double pressure = std::tanh(net / static_cast<long double>(config_.liquidity));
    const long double calculated = static_cast<long double>(config_.intrinsic_price) *
        (1.0L + static_cast<long double>(config_.max_deviation) * pressure);
    if (!std::isfinite(calculated) || calculated <= 0 || calculated > std::numeric_limits<Price>::max())
        throw std::overflow_error("calculated price is out of range for " + config_.ticker);
    return static_cast<Price>(std::llround(calculated));
}

Price Market::apply(OrderSide side, Quantity quantity) {
    const Price next = projected_price(side, quantity);
    if (side == OrderSide::Buy) buy_quantity_ += quantity;
    else sell_quantity_ += quantity;
    current_price_ = next;
    return next;
}

void Market::pend(Order order) {
    if (!order.limit_price) throw std::logic_error("market order cannot enter pending pool");
    if (order.side == OrderSide::Buy) buys_.push(std::move(order));
    else sells_.push(std::move(order));
}

std::optional<Order> Market::take_eligible(OrderSide side) {
    if (side == OrderSide::Buy) {
        if (buys_.empty() || !eligible(buys_.top())) return std::nullopt;
        Order result = buys_.top(); buys_.pop(); return result;
    }
    if (sells_.empty() || !eligible(sells_.top())) return std::nullopt;
    Order result = sells_.top(); sells_.pop(); return result;
}

std::vector<Order> Market::take_all_pending() {
    std::vector<Order> result;
    result.reserve(pending_count());
    while (!buys_.empty()) { result.push_back(buys_.top()); buys_.pop(); }
    while (!sells_.empty()) { result.push_back(sells_.top()); sells_.pop(); }
    return result;
}

Engine::Engine(const SimulationConfig& config, BoundedMpscQueue<Order>& queue,
               std::atomic_bool& stop_requested, std::atomic_bool& fatal_error)
    : config_(config), queue_(queue), stop_requested_(stop_requested), fatal_error_(fatal_error) {
    for (const auto& symbol : config.symbols) markets_.emplace(symbol.ticker, Market(symbol));
}

RunMetrics Engine::run() {
    try {
        ledger_ = std::make_unique<Ledger>(config_.database_path, config_.commit_batch_size, effective_config_json(config_));
        while (auto order = queue_.pop()) process(std::move(*order));
        expire_pending();
        verify_invariants();
        for (const auto& [ticker, market] : markets_) {
            SymbolMetrics item{ticker, market.current_price(), market.config().price_scale,
                               market.buy_quantity(), market.sell_quantity()};
            metrics_.symbols.push_back(item);
            ledger_->record_symbol_summary(item);
        }
        metrics_.transactions = ledger_->transaction_count() + 1;
        ledger_->finish(stop_requested_.load(std::memory_order_relaxed) ? "INTERRUPTED" : "COMPLETED");
        metrics_.interrupted = stop_requested_.load(std::memory_order_relaxed);
    } catch (const std::exception& error) {
        metrics_.failed = true;
        metrics_.error = error.what();
        fatal_error_.store(true, std::memory_order_relaxed);
        stop_requested_.store(true, std::memory_order_relaxed);
        if (ledger_) ledger_->rollback();
        queue_.close();
    }
    return metrics_;
}

void Engine::process(Order order) {
    ++metrics_.processed;
    ++metrics_.accepted;
    auto it = markets_.find(order.symbol);
    if (it == markets_.end()) throw std::invalid_argument("unknown order symbol: " + order.symbol);
    if (order.quantity == 0) throw std::invalid_argument("zero-quantity order");

    if (order.type == OrderType::Market) {
        if (order.limit_price) throw std::invalid_argument("market order has a limit price");
        ++metrics_.market_orders;
        ledger_->record_order(order, OrderStatus::Accepted);
        execute(std::move(order), true);
    } else {
        if (!order.limit_price || *order.limit_price <= 0) throw std::invalid_argument("limit order has invalid price");
        ++metrics_.limit_orders;
        if (it->second.eligible(order)) {
            ledger_->record_order(order, OrderStatus::Accepted);
            execute(std::move(order), false);
        } else {
            ledger_->record_order(order, OrderStatus::Pending);
            it->second.pend(std::move(order));
        }
    }
}

void Engine::execute(Order order, bool market_order) {
    Market& market = markets_.at(order.symbol);
    const Price old_price = market.current_price();
    const Price fill_price = market_order ? market.projected_price(order.side, order.quantity) : old_price;
    market.apply(order.side, order.quantity);
    const std::uint64_t sequence = ++engine_sequence_;
    Execution execution{next_execution_id_++, order.id, order.symbol, order.side, order.quantity, fill_price, sequence};
    ledger_->record_execution(execution);
    ledger_->update_status(order.id, OrderStatus::Filled, sequence);
    ++metrics_.filled;
    ++metrics_.executions;
    cascade(market, old_price);
}

void Engine::cascade(Market& market, Price old_price) {
    if (market.current_price() == old_price) return;
    OrderSide triggered = market.current_price() > old_price ? OrderSide::Sell : OrderSide::Buy;
    while (true) {
        auto next = market.take_eligible(triggered);
        // A fill can reverse the price without making all orders on the previous
        // side ineligible. Check the opposite pool before declaring stability.
        if (!next) {
            const OrderSide opposite = triggered == OrderSide::Buy ? OrderSide::Sell : OrderSide::Buy;
            next = market.take_eligible(opposite);
        }
        if (!next) return;
        old_price = market.current_price();
        // Do not recurse: a pending fill is recorded here and the loop follows any reversal.
        const Price fill_price = old_price;
        market.apply(next->side, next->quantity);
        if (market.current_price() > old_price) triggered = OrderSide::Sell;
        else if (market.current_price() < old_price) triggered = OrderSide::Buy;
        const std::uint64_t sequence = ++engine_sequence_;
        ledger_->record_execution({next_execution_id_++, next->id, next->symbol, next->side,
                                   next->quantity, fill_price, sequence});
        ledger_->update_status(next->id, OrderStatus::Filled, sequence);
        ++metrics_.filled;
        ++metrics_.executions;
    }
}

void Engine::expire_pending() {
    for (auto& [ignored, market] : markets_) {
        (void)ignored;
        for (const auto& order : market.take_all_pending()) {
            ledger_->update_status(order.id, OrderStatus::Expired, ++engine_sequence_);
            ++metrics_.expired;
        }
    }
}

void Engine::verify_invariants() const {
    if (metrics_.accepted != metrics_.processed) throw std::logic_error("accepted/processed accounting mismatch");
    if (metrics_.accepted != metrics_.filled + metrics_.expired) throw std::logic_error("orders did not reach terminal states");
    if (metrics_.filled != metrics_.executions) throw std::logic_error("fill/execution accounting mismatch");
    for (const auto& [ticker, market] : markets_) {
        if (market.pending_count() != 0) throw std::logic_error("pending orders remain for " + ticker);
        const long double low = static_cast<long double>(market.config().intrinsic_price) * (1.0L - market.config().max_deviation);
        const long double high = static_cast<long double>(market.config().intrinsic_price) * (1.0L + market.config().max_deviation);
        if (market.current_price() < std::floor(low) || market.current_price() > std::ceil(high))
            throw std::logic_error("price bound violated for " + ticker);
    }
}

} // namespace amm
