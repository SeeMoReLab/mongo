/**
 * Slot-time occupancy accumulator (DESIGN.md step 5): every request holds a
 * slot from admission to reply, and the code that changes the in-flight count
 * is the code that records how long the previous count lasted. The interval
 * average is exactly time-weighted; no sampling is involved.
 *
 * The clock is injectable for tests. Standard library only.
 */
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>

namespace mongo::chameleon {

class OccupancyMeter {
public:
    /** One closed control interval. */
    struct Interval {
        int64_t slotNanos;
        int64_t intervalNanos;
        int inFlightAtClose;

        /** Time-weighted average number of requests in flight. */
        double averageInFlight() const {
            return intervalNanos <= 0 ? 0.0 : static_cast<double>(slotNanos) / intervalNanos;
        }
    };

    using Clock = std::function<int64_t()>;

    static int64_t steadyNanos() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    OccupancyMeter() : OccupancyMeter(&OccupancyMeter::steadyNanos) {}

    explicit OccupancyMeter(Clock clock) : _clock(std::move(clock)) {
        int64_t now = _clock();
        _lastEventNanos = now;
        _intervalStartNanos = now;
    }

    /** +1 on admission, -1 on reply. */
    void onEvent(int delta) {
        std::lock_guard<std::mutex> lock(_mutex);
        int64_t t = _clock();
        _accumulatedSlotNanos += static_cast<int64_t>(_inFlight) * (t - _lastEventNanos);
        _inFlight += delta;
        _lastEventNanos = t;
    }

    /** Close out the running stretch and start a new interval (control boundary). */
    Interval closeInterval() {
        std::lock_guard<std::mutex> lock(_mutex);
        int64_t t = _clock();
        _accumulatedSlotNanos += static_cast<int64_t>(_inFlight) * (t - _lastEventNanos);
        _lastEventNanos = t;
        Interval interval{_accumulatedSlotNanos, t - _intervalStartNanos, _inFlight};
        _accumulatedSlotNanos = 0;
        _intervalStartNanos = t;
        return interval;
    }

    int inFlight() const {
        std::lock_guard<std::mutex> lock(_mutex);
        return _inFlight;
    }

private:
    Clock _clock;
    mutable std::mutex _mutex;
    int64_t _accumulatedSlotNanos = 0;
    int _inFlight = 0;
    int64_t _lastEventNanos;
    int64_t _intervalStartNanos;
};

}  // namespace mongo::chameleon
