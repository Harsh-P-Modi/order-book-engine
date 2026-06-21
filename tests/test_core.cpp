#include "amm/bounded_queue.hpp"
#include "amm/config.hpp"
#include "amm/engine.hpp"

#include <catch2/catch_test_macros.hpp>
#include <sqlite3.h>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <thread>

using namespace std::chrono_literals;

namespace {
std::int64_t scalar(sqlite3* db, const char* sql) {
    sqlite3_stmt* statement{};
    if (sqlite3_prepare_v2(db, sql, -1, &statement, nullptr) != SQLITE_OK) throw std::runtime_error(sqlite3_errmsg(db));
    if (sqlite3_step(statement) != SQLITE_ROW) { sqlite3_finalize(statement); throw std::runtime_error("query returned no row"); }
    const auto value = sqlite3_column_int64(statement, 0);
    sqlite3_finalize(statement);
    return value;
}
}

TEST_CASE("bounded queue is FIFO and assigns enqueue sequence") {
    amm::BoundedMpscQueue<amm::Order> queue(2);
    std::atomic_bool stop{false};
    amm::Order first; first.id = 1;
    amm::Order second; second.id = 2;
    REQUIRE(queue.push(first, stop));
    REQUIRE(queue.push(second, stop));
    queue.close();
    const auto a = queue.pop();
    const auto b = queue.pop();
    REQUIRE(a->id == 1);
    REQUIRE(b->id == 2);
    REQUIRE(a->enqueue_sequence == 1);
    REQUIRE(b->enqueue_sequence == 2);
    REQUIRE_FALSE(queue.pop());
    REQUIRE(queue.high_water_mark() == 2);
}

TEST_CASE("bounded queue applies backpressure") {
    amm::BoundedMpscQueue<int> queue(1);
    std::atomic_bool stop{false};
    REQUIRE(queue.push(1, stop));
    std::atomic_bool pushed{false};
    std::thread producer([&] { pushed = queue.push(2, stop); });
    std::this_thread::sleep_for(20ms);
    REQUIRE_FALSE(pushed.load());
    REQUIRE(queue.pop() == 1);
    producer.join();
    REQUIRE(pushed.load());
    REQUIRE(queue.blocked_pushes() == 1);
    queue.close();
}

TEST_CASE("AMM price is bounded and directionally monotonic") {
    amm::SymbolConfig config{"TEST", 10'000, 1'000, 0.5, 100};
    amm::Market market(config);
    REQUIRE(market.current_price() == 10'000);
    const auto up = market.apply(amm::OrderSide::Buy, 500);
    REQUIRE(up > 10'000);
    REQUIRE(up <= 15'000);
    const auto down = market.apply(amm::OrderSide::Sell, 2'000);
    REQUIRE(down < 10'000);
    REQUIRE(down >= 5'000);
}

TEST_CASE("pending orders use price-time priority") {
    amm::Market market({"TEST", 10'000, 1'000, 0.5, 100});
    amm::Order later{2, 0, 2, 2, "TEST", amm::OrderSide::Buy, amm::OrderType::Limit, 1, 9'000};
    amm::Order better{3, 0, 3, 3, "TEST", amm::OrderSide::Buy, amm::OrderType::Limit, 1, 9'500};
    amm::Order earlier{1, 0, 1, 1, "TEST", amm::OrderSide::Buy, amm::OrderType::Limit, 1, 9'500};
    market.pend(later); market.pend(better); market.pend(earlier);
    market.apply(amm::OrderSide::Sell, 1'000);
    REQUIRE(market.take_eligible(amm::OrderSide::Buy)->id == 1);
    REQUIRE(market.take_eligible(amm::OrderSide::Buy)->id == 3);
    REQUIRE(market.take_eligible(amm::OrderSide::Buy)->id == 2);
}

TEST_CASE("configuration file is overridden by CLI") {
    const auto path = std::filesystem::temp_directory_path() / "amm-config-test.json";
    { std::ofstream out(path); out << R"({"traders":2,"orders_per_trader":3})"; }
    const std::string path_string = path.string();
    std::vector<std::string> values{"amm-sim", "--config", path_string, "--traders", "4"};
    std::vector<char*> args;
    for (auto& value : values) args.push_back(value.data());
    const auto result = amm::load_configuration(static_cast<int>(args.size()), args.data());
    REQUIRE(result.config.traders == 4);
    REQUIRE(result.config.orders_per_trader == 3);
    std::filesystem::remove(path);
}

TEST_CASE("configuration rejects unknown keys and invalid markets") {
    amm::SimulationConfig config;
    config.symbols.front().max_deviation = 1.0;
    REQUIRE_THROWS_AS(amm::validate(config), std::invalid_argument);

    const auto path = std::filesystem::temp_directory_path() / "amm-bad-config-test.json";
    { std::ofstream out(path); out << R"({"mystery":1})"; }
    const std::string path_string = path.string();
    std::vector<std::string> values{"amm-sim", "--config", path_string};
    std::vector<char*> args;
    for (auto& value : values) args.push_back(value.data());
    REQUIRE_THROWS_AS(amm::load_configuration(static_cast<int>(args.size()), args.data()), std::invalid_argument);
    std::filesystem::remove(path);
}

TEST_CASE("engine persists every order to a terminal state") {
    const auto db = std::filesystem::temp_directory_path() / "amm-engine-test.db";
    std::filesystem::remove(db);
    amm::SimulationConfig config;
    config.database_path = db.string();
    config.commit_batch_size = 2;
    config.symbols = {{"TEST", 10'000, 1'000, 0.5, 100}};
    amm::BoundedMpscQueue<amm::Order> queue(8);
    std::atomic_bool stop{false};
    std::atomic_bool fatal{false};
    REQUIRE(queue.push({1, 0, 1, 0, "TEST", amm::OrderSide::Buy, amm::OrderType::Market, 100, std::nullopt}, stop));
    REQUIRE(queue.push({2, 0, 2, 0, "TEST", amm::OrderSide::Buy, amm::OrderType::Limit, 10, 1}, stop));
    queue.close();
    {
        amm::Engine engine(config, queue, stop, fatal);
        const auto metrics = engine.run();
        REQUIRE_FALSE(metrics.failed);
        REQUIRE(metrics.accepted == 2);
        REQUIRE(metrics.filled == 1);
        REQUIRE(metrics.expired == 1);
        REQUIRE(metrics.executions == 1);
    }
    sqlite3* handle{};
    REQUIRE(sqlite3_open_v2(db.string().c_str(), &handle, SQLITE_OPEN_READONLY, nullptr) == SQLITE_OK);
    REQUIRE(scalar(handle, "SELECT COUNT(*) FROM orders") == 2);
    REQUIRE(scalar(handle, "SELECT COUNT(*) FROM orders WHERE status IN ('FILLED','EXPIRED')") == 2);
    REQUIRE(scalar(handle, "SELECT COUNT(*) FROM executions") == 1);
    REQUIRE(scalar(handle, "SELECT COUNT(*) FROM runs WHERE status='COMPLETED'") == 1);
    sqlite3_close(handle);
    std::filesystem::remove(db);
}

TEST_CASE("engine reports persistence failure") {
    amm::SimulationConfig config;
#ifdef _WIN32
    config.database_path = "Z:/this/path/does/not/exist/ledger.db";
#else
    config.database_path = "/this/path/does/not/exist/ledger.db";
#endif
    amm::BoundedMpscQueue<amm::Order> queue(1);
    queue.close();
    std::atomic_bool stop{false};
    std::atomic_bool fatal{false};
    amm::Engine engine(config, queue, stop, fatal);
    const auto metrics = engine.run();
    REQUIRE(metrics.failed);
    REQUIRE(fatal.load());
}

TEST_CASE("CLI completes and creates an auditable ledger", "[e2e]") {
    const auto db = std::filesystem::temp_directory_path() / "amm-cli-e2e.db";
    std::filesystem::remove(db);
#ifdef _WIN32
    const std::string command = std::string("cmd /c \"\"") + AMM_SIM_PATH +
        "\" --traders 2 --orders-per-trader 50 --queue-capacity 8 --commit-batch-size 7 --database \"" +
        db.string() + "\"\"";
#else
    const std::string command = std::string("\"") + AMM_SIM_PATH +
        "\" --traders 2 --orders-per-trader 50 --queue-capacity 8 --commit-batch-size 7 --database \"" +
        db.string() + "\"";
#endif
    REQUIRE(std::system(command.c_str()) == 0);

    sqlite3* handle{};
    REQUIRE(sqlite3_open_v2(db.string().c_str(), &handle, SQLITE_OPEN_READONLY, nullptr) == SQLITE_OK);
    REQUIRE(scalar(handle, "SELECT COUNT(*) FROM orders") == 100);
    REQUIRE(scalar(handle, "SELECT COUNT(*) FROM orders WHERE status NOT IN ('FILLED','EXPIRED')") == 0);
    REQUIRE(scalar(handle, "SELECT COUNT(*) FROM runs WHERE status='COMPLETED'") == 1);
    REQUIRE(scalar(handle, "SELECT COUNT(*) FROM executions") ==
            scalar(handle, "SELECT COUNT(*) FROM orders WHERE status='FILLED'"));
    sqlite3_close(handle);
    std::filesystem::remove(db);
}
