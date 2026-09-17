/**
 * The replication resource (DESIGN.md resource model): a rate in oplog
 * entries per second, refilled continuously, charged at admission. In
 * MongoDB a write produces one entry per modified document, unknown until the
 * predicate has run, so the charge is an estimate at admission and a true-up
 * at completion (trueUp with the difference between actual and estimated
 * entries; negative refunds). Linearizable reads charge one entry each for
 * their confirmation noop. An empty bucket is the hard backstop for writes.
 *
 * The clock is injectable for tests. Standard library only.
 */
#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>

namespace mongo::chameleon {

class ReplicationRateBucket {
public:
    using Clock = std::function<int64_t()>;

    static int64_t steadyNanos() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    explicit ReplicationRateBucket(double entriesPerSecond)
        : ReplicationRateBucket(entriesPerSecond, &ReplicationRateBucket::steadyNanos) {}

    ReplicationRateBucket(double entriesPerSecond, Clock clock)
        : _entriesPerSecond(entriesPerSecond),
          _clock(std::move(clock)),
          _tokens(entriesPerSecond),
          _lastRefillNanos(_clock()) {}

    /**
     * Charge 'entries' (at least one). False = not enough tokens; the write
     * must be rejected. A partial charge is never made.
     */
    bool tryCharge(double entries) {
        std::lock_guard<std::mutex> lock(_mutex);
        refill();
        double cost = std::max(1.0, entries);
        if (_tokens >= cost) {
            _tokens -= cost;
            return true;
        }
        return false;
    }

    /**
     * Reconcile an admitted operation's estimate with what it actually
     * replicated: charges the excess, refunds the shortfall. The bucket never
     * goes negative (an over-run is absorbed by later admissions) and never
     * exceeds its capacity.
     */
    void trueUp(double estimatedEntries, double actualEntries) {
        std::lock_guard<std::mutex> lock(_mutex);
        refill();
        _tokens = std::min(_entriesPerSecond, std::max(0.0, _tokens - (actualEntries - estimatedEntries)));
    }

    double tokensRemaining() {
        std::lock_guard<std::mutex> lock(_mutex);
        refill();
        return _tokens;
    }

    void reconfigure(double entriesPerSecond) {
        std::lock_guard<std::mutex> lock(_mutex);
        refill();
        _entriesPerSecond = entriesPerSecond;
        _tokens = std::min(_tokens, entriesPerSecond);
    }

private:
    void refill() {
        int64_t now = _clock();
        _tokens = std::min(_entriesPerSecond,
                           _tokens + static_cast<double>(now - _lastRefillNanos) / 1e9 * _entriesPerSecond);
        _lastRefillNanos = now;
    }

    double _entriesPerSecond;
    Clock _clock;
    std::mutex _mutex;
    double _tokens;
    int64_t _lastRefillNanos;
};

}  // namespace mongo::chameleon
