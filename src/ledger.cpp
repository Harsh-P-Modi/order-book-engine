#include "amm/ledger.hpp"

#include <sqlite3.h>
#include <stdexcept>
#include <string>

namespace amm {
namespace {

void require_ok(sqlite3* db, int code, const char* action) {
    if (code != SQLITE_OK && code != SQLITE_DONE && code != SQLITE_ROW) {
        throw std::runtime_error(std::string(action) + ": " + sqlite3_errmsg(db));
    }
}

void reset(sqlite3* db, sqlite3_stmt* statement) {
    require_ok(db, sqlite3_step(statement), "SQLite statement failed");
    require_ok(db, sqlite3_reset(statement), "SQLite reset failed");
    require_ok(db, sqlite3_clear_bindings(statement), "SQLite clear bindings failed");
}

} // namespace

Ledger::Ledger(const std::string& path, std::uint32_t batch_size, const std::string& config_json)
    : batch_size_(batch_size) {
    if (sqlite3_open_v2(path.c_str(), &db_, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr) != SQLITE_OK) {
        const std::string message = db_ ? sqlite3_errmsg(db_) : "allocation failed";
        if (db_) sqlite3_close(db_);
        db_ = nullptr;
        throw std::runtime_error("cannot open SQLite ledger: " + message);
    }
    try {
        execute("PRAGMA foreign_keys=ON");
        execute("PRAGMA journal_mode=WAL");
        execute("PRAGMA synchronous=NORMAL");
        execute("CREATE TABLE IF NOT EXISTS runs("
                "id INTEGER PRIMARY KEY, started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, finished_at TEXT,"
                "status TEXT NOT NULL, config_json TEXT NOT NULL)");
        execute("CREATE TABLE IF NOT EXISTS orders("
                "run_id INTEGER NOT NULL, order_id INTEGER NOT NULL, producer_id INTEGER NOT NULL, producer_sequence INTEGER NOT NULL,"
                "enqueue_sequence INTEGER NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL, type TEXT NOT NULL, quantity INTEGER NOT NULL,"
                "limit_price INTEGER, status TEXT NOT NULL, terminal_engine_sequence INTEGER,"
                "PRIMARY KEY(run_id, order_id), FOREIGN KEY(run_id) REFERENCES runs(id))");
        execute("CREATE TABLE IF NOT EXISTS executions("
                "run_id INTEGER NOT NULL, execution_id INTEGER NOT NULL, order_id INTEGER NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL,"
                "quantity INTEGER NOT NULL, price INTEGER NOT NULL, engine_sequence INTEGER NOT NULL, PRIMARY KEY(run_id, execution_id),"
                "FOREIGN KEY(run_id, order_id) REFERENCES orders(run_id, order_id))");
        execute("CREATE TABLE IF NOT EXISTS symbol_summaries("
                "run_id INTEGER NOT NULL, symbol TEXT NOT NULL, final_price INTEGER NOT NULL, price_scale INTEGER NOT NULL,"
                "buy_quantity INTEGER NOT NULL, sell_quantity INTEGER NOT NULL, PRIMARY KEY(run_id, symbol),"
                "FOREIGN KEY(run_id) REFERENCES runs(id))");

        sqlite3_stmt* run = prepare("INSERT INTO runs(status, config_json) VALUES('RUNNING', ?)");
        require_ok(db_, sqlite3_bind_text(run, 1, config_json.c_str(), -1, SQLITE_TRANSIENT), "bind run config");
        require_ok(db_, sqlite3_step(run), "insert run");
        sqlite3_finalize(run);
        run_id_ = sqlite3_last_insert_rowid(db_);

        insert_order_ = prepare("INSERT INTO orders VALUES(?,?,?,?,?,?,?,?,?,?,?,NULL)");
        update_order_ = prepare("UPDATE orders SET status=?, terminal_engine_sequence=? WHERE run_id=? AND order_id=?");
        insert_execution_ = prepare("INSERT INTO executions VALUES(?,?,?,?,?,?,?,?)");
        insert_summary_ = prepare("INSERT INTO symbol_summaries VALUES(?,?,?,?,?,?)");
        begin();
    } catch (...) {
        finalize_all();
        sqlite3_close(db_);
        db_ = nullptr;
        throw;
    }
}

Ledger::~Ledger() {
    if (!db_) return;
    if (transaction_open_) rollback();
    finalize_all();
    sqlite3_close(db_);
}

void Ledger::execute(const char* sql) {
    char* error{};
    const int result = sqlite3_exec(db_, sql, nullptr, nullptr, &error);
    if (result != SQLITE_OK) {
        const std::string message = error ? error : sqlite3_errmsg(db_);
        sqlite3_free(error);
        throw std::runtime_error("SQLite operation failed: " + message);
    }
}

sqlite3_stmt* Ledger::prepare(const char* sql) {
    sqlite3_stmt* statement{};
    require_ok(db_, sqlite3_prepare_v2(db_, sql, -1, &statement, nullptr), "prepare statement");
    return statement;
}

void Ledger::begin() { execute("BEGIN IMMEDIATE"); transaction_open_ = true; events_in_batch_ = 0; }

void Ledger::commit(bool reopen) {
    if (!transaction_open_) return;
    execute("COMMIT");
    transaction_open_ = false;
    ++transaction_count_;
    events_in_batch_ = 0;
    if (reopen) begin();
}

void Ledger::count_event() {
    ++events_in_batch_;
    if (events_in_batch_ >= batch_size_) commit(true);
}

void Ledger::record_order(const Order& o, OrderStatus status) {
    require_ok(db_, sqlite3_bind_int64(insert_order_, 1, run_id_), "bind run id");
    require_ok(db_, sqlite3_bind_int64(insert_order_, 2, static_cast<sqlite3_int64>(o.id)), "bind order id");
    require_ok(db_, sqlite3_bind_int(insert_order_, 3, static_cast<int>(o.producer_id)), "bind producer id");
    require_ok(db_, sqlite3_bind_int(insert_order_, 4, static_cast<int>(o.producer_sequence)), "bind producer sequence");
    require_ok(db_, sqlite3_bind_int64(insert_order_, 5, static_cast<sqlite3_int64>(o.enqueue_sequence)), "bind enqueue sequence");
    require_ok(db_, sqlite3_bind_text(insert_order_, 6, o.symbol.c_str(), -1, SQLITE_TRANSIENT), "bind symbol");
    require_ok(db_, sqlite3_bind_text(insert_order_, 7, to_string(o.side), -1, SQLITE_STATIC), "bind side");
    require_ok(db_, sqlite3_bind_text(insert_order_, 8, to_string(o.type), -1, SQLITE_STATIC), "bind type");
    require_ok(db_, sqlite3_bind_int64(insert_order_, 9, static_cast<sqlite3_int64>(o.quantity)), "bind quantity");
    if (o.limit_price) require_ok(db_, sqlite3_bind_int64(insert_order_, 10, *o.limit_price), "bind limit price");
    else require_ok(db_, sqlite3_bind_null(insert_order_, 10), "bind null limit");
    require_ok(db_, sqlite3_bind_text(insert_order_, 11, to_string(status), -1, SQLITE_STATIC), "bind status");
    reset(db_, insert_order_);
    count_event();
}

void Ledger::update_status(std::uint64_t order_id, OrderStatus status, std::uint64_t sequence) {
    require_ok(db_, sqlite3_bind_text(update_order_, 1, to_string(status), -1, SQLITE_STATIC), "bind status");
    require_ok(db_, sqlite3_bind_int64(update_order_, 2, static_cast<sqlite3_int64>(sequence)), "bind terminal sequence");
    require_ok(db_, sqlite3_bind_int64(update_order_, 3, run_id_), "bind run id");
    require_ok(db_, sqlite3_bind_int64(update_order_, 4, static_cast<sqlite3_int64>(order_id)), "bind order id");
    reset(db_, update_order_);
    count_event();
}

void Ledger::record_execution(const Execution& e) {
    require_ok(db_, sqlite3_bind_int64(insert_execution_, 1, run_id_), "bind run id");
    require_ok(db_, sqlite3_bind_int64(insert_execution_, 2, static_cast<sqlite3_int64>(e.id)), "bind execution id");
    require_ok(db_, sqlite3_bind_int64(insert_execution_, 3, static_cast<sqlite3_int64>(e.order_id)), "bind order id");
    require_ok(db_, sqlite3_bind_text(insert_execution_, 4, e.symbol.c_str(), -1, SQLITE_TRANSIENT), "bind symbol");
    require_ok(db_, sqlite3_bind_text(insert_execution_, 5, to_string(e.side), -1, SQLITE_STATIC), "bind side");
    require_ok(db_, sqlite3_bind_int64(insert_execution_, 6, static_cast<sqlite3_int64>(e.quantity)), "bind quantity");
    require_ok(db_, sqlite3_bind_int64(insert_execution_, 7, e.price), "bind price");
    require_ok(db_, sqlite3_bind_int64(insert_execution_, 8, static_cast<sqlite3_int64>(e.engine_sequence)), "bind engine sequence");
    reset(db_, insert_execution_);
    count_event();
}

void Ledger::record_symbol_summary(const SymbolMetrics& m) {
    require_ok(db_, sqlite3_bind_int64(insert_summary_, 1, run_id_), "bind run id");
    require_ok(db_, sqlite3_bind_text(insert_summary_, 2, m.symbol.c_str(), -1, SQLITE_TRANSIENT), "bind symbol");
    require_ok(db_, sqlite3_bind_int64(insert_summary_, 3, m.final_price), "bind price");
    require_ok(db_, sqlite3_bind_int(insert_summary_, 4, static_cast<int>(m.price_scale)), "bind scale");
    require_ok(db_, sqlite3_bind_int64(insert_summary_, 5, static_cast<sqlite3_int64>(m.buy_quantity)), "bind buys");
    require_ok(db_, sqlite3_bind_int64(insert_summary_, 6, static_cast<sqlite3_int64>(m.sell_quantity)), "bind sells");
    reset(db_, insert_summary_);
    count_event();
}

void Ledger::finish(const std::string& status) {
    commit(false);
    sqlite3_stmt* statement = prepare("UPDATE runs SET status=?, finished_at=CURRENT_TIMESTAMP WHERE id=?");
    require_ok(db_, sqlite3_bind_text(statement, 1, status.c_str(), -1, SQLITE_TRANSIENT), "bind run status");
    require_ok(db_, sqlite3_bind_int64(statement, 2, run_id_), "bind run id");
    require_ok(db_, sqlite3_step(statement), "finish run");
    sqlite3_finalize(statement);
}

void Ledger::rollback() noexcept {
    if (db_ && transaction_open_) {
        sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);
        transaction_open_ = false;
    }
}

void Ledger::finalize_all() noexcept {
    sqlite3_finalize(insert_order_); insert_order_ = nullptr;
    sqlite3_finalize(update_order_); update_order_ = nullptr;
    sqlite3_finalize(insert_execution_); insert_execution_ = nullptr;
    sqlite3_finalize(insert_summary_); insert_summary_ = nullptr;
}

} // namespace amm
