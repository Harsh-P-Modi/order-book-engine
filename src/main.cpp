#include "amm/bounded_queue.hpp"
#include "amm/config.hpp"
#include "amm/engine.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

namespace {
std::atomic_bool global_stop_requested{false};

extern "C" void handle_interrupt(int) {
    global_stop_requested.store(true, std::memory_order_relaxed);
}

std::uint64_t splitmix64(std::uint64_t value) {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31U);
}

void produce(std::uint32_t producer_id, const amm::SimulationConfig& config,
             amm::BoundedMpscQueue<amm::Order>& queue, std::atomic_bool& stop,
             std::atomic<std::uint64_t>& generated) {
    std::mt19937_64 rng(splitmix64(config.seed ^ producer_id));
    std::bernoulli_distribution market(config.market_order_probability);
    std::bernoulli_distribution buy(config.buy_probability);
    std::uniform_int_distribution<amm::Quantity> quantity(config.min_quantity, config.max_quantity);
    std::uniform_int_distribution<std::size_t> symbol_index(0, config.symbols.size() - 1);
    std::uniform_real_distribution<double> offset(-config.limit_price_band, config.limit_price_band);

    for (std::uint32_t sequence = 1; sequence <= config.orders_per_trader; ++sequence) {
        if (stop.load(std::memory_order_relaxed)) break;
        const auto& symbol = config.symbols[symbol_index(rng)];
        amm::Order order;
        order.id = (static_cast<std::uint64_t>(producer_id + 1U) << 32U) | sequence;
        order.producer_id = producer_id;
        order.producer_sequence = sequence;
        order.symbol = symbol.ticker;
        order.side = buy(rng) ? amm::OrderSide::Buy : amm::OrderSide::Sell;
        order.type = market(rng) ? amm::OrderType::Market : amm::OrderType::Limit;
        order.quantity = quantity(rng);
        if (order.type == amm::OrderType::Limit) {
            const double base = amm::display_price(symbol.intrinsic_price, symbol.price_scale);
            order.limit_price = amm::parse_price(base * (1.0 + offset(rng)), symbol.price_scale);
        }
        generated.fetch_add(1, std::memory_order_relaxed);
        if (!queue.push(std::move(order), stop)) break;
    }
}

void print_report(const amm::RunMetrics& m, const amm::SimulationConfig& config) {
    const double throughput = m.elapsed_seconds > 0 ? static_cast<double>(m.processed) / m.elapsed_seconds : 0;
    std::cout << "\nAMM simulation " << (m.failed ? "FAILED" : (m.interrupted ? "INTERRUPTED" : "COMPLETED")) << '\n'
              << "  generated:       " << m.generated << '\n'
              << "  accepted:        " << m.accepted << '\n'
              << "  processed:       " << m.processed << '\n'
              << "  filled/expired:  " << m.filled << " / " << m.expired << '\n'
              << "  market/limit:    " << m.market_orders << " / " << m.limit_orders << '\n'
              << "  queue high-water:" << ' ' << m.queue_high_water << '\n'
              << "  blocked pushes:  " << m.blocked_pushes << '\n'
              << "  transactions:    " << m.transactions << '\n'
              << "  elapsed seconds: " << std::fixed << std::setprecision(3) << m.elapsed_seconds << '\n'
              << "  orders/second:   " << std::fixed << std::setprecision(0) << throughput << '\n'
              << "  database:        " << config.database_path << '\n';
    for (const auto& s : m.symbols) {
        std::cout << "  " << s.symbol << ": price=" << std::fixed << std::setprecision(6)
                  << amm::display_price(s.final_price, s.price_scale)
                  << ", buy_qty=" << s.buy_quantity << ", sell_qty=" << s.sell_quantity << '\n';
    }
    if (m.failed) std::cerr << "  error: " << m.error << '\n';
}
} // namespace

int main(int argc, char** argv) {
    try {
        const auto cli = amm::load_configuration(argc, argv);
        if (cli.show_help) { amm::print_help(std::cout); return 0; }
        const auto& config = cli.config;

        amm::BoundedMpscQueue<amm::Order> queue(config.queue_capacity);
        std::atomic_bool fatal_error{false};
        std::atomic<std::uint64_t> generated{0};
        global_stop_requested.store(false, std::memory_order_relaxed);
        std::signal(SIGINT, handle_interrupt);

        amm::Engine engine(config, queue, global_stop_requested, fatal_error);
        amm::RunMetrics metrics;
        const auto started = std::chrono::steady_clock::now();
        std::vector<std::thread> producers;
        producers.reserve(config.traders);
        std::thread consumer([&] { metrics = engine.run(); });
        try {
            for (std::uint32_t id = 0; id < config.traders; ++id) {
                producers.emplace_back(produce, id, std::cref(config), std::ref(queue),
                                       std::ref(global_stop_requested), std::ref(generated));
            }
        } catch (...) {
            global_stop_requested.store(true, std::memory_order_relaxed);
            queue.close();
            for (auto& producer : producers) producer.join();
            consumer.join();
            throw;
        }
        for (auto& producer : producers) producer.join();
        queue.close();
        consumer.join();

        metrics.generated = generated.load(std::memory_order_relaxed);
        metrics.queue_high_water = queue.high_water_mark();
        metrics.blocked_pushes = queue.blocked_pushes();
        metrics.elapsed_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
        print_report(metrics, config);
        return metrics.failed || fatal_error.load(std::memory_order_relaxed) ? 1 : 0;
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        amm::print_help(std::cerr);
        return 2;
    }
}
