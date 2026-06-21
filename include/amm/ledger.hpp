#pragma once

#include "amm/types.hpp"
#include <cstdint>
#include <string>

struct sqlite3;
struct sqlite3_stmt;

namespace amm {

class Ledger {
public:
    Ledger(const std::string& path, std::uint32_t batch_size, const std::string& config_json);
    ~Ledger();
    Ledger(const Ledger&) = delete;
    Ledger& operator=(const Ledger&) = delete;

    void record_order(const Order& order, OrderStatus status);
    void update_status(std::uint64_t order_id, OrderStatus status, std::uint64_t engine_sequence);
    void record_execution(const Execution& execution);
    void record_symbol_summary(const SymbolMetrics& metrics);
    void finish(const std::string& status);
    void rollback() noexcept;
    [[nodiscard]] std::uint64_t transaction_count() const { return transaction_count_; }
    [[nodiscard]] std::int64_t run_id() const { return run_id_; }

private:
    void execute(const char* sql);
    sqlite3_stmt* prepare(const char* sql);
    void begin();
    void count_event();
    void commit(bool reopen);
    void finalize_all() noexcept;

    sqlite3* db_{};
    sqlite3_stmt* insert_order_{};
    sqlite3_stmt* update_order_{};
    sqlite3_stmt* insert_execution_{};
    sqlite3_stmt* insert_summary_{};
    std::uint32_t batch_size_{};
    std::uint32_t events_in_batch_{};
    std::uint64_t transaction_count_{};
    std::int64_t run_id_{};
    bool transaction_open_{};
};

} // namespace amm
