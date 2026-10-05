#include "mongo/db/chameleon/service_time_histograms.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace mongo::chameleon {

namespace {
const double kLogRatio = std::log(HistogramSnapshot::kBucketRatio);
}  // namespace

// ===== HistogramSnapshot =====

std::size_t HistogramSnapshot::latencyBucketOf(double ms) {
    if (ms <= kMinLatencyMs) {
        return 0;
    }
    auto index = static_cast<std::size_t>(std::floor(std::log(ms / kMinLatencyMs) / kLogRatio));
    return std::min(index, kLatencyBuckets - 1);
}

double HistogramSnapshot::lowerEdgeMs(std::size_t bucket) {
    return bucket == 0 ? 0.0 : kMinLatencyMs * std::pow(kBucketRatio, static_cast<double>(bucket));
}

double HistogramSnapshot::upperEdgeMs(std::size_t bucket) {
    return kMinLatencyMs * std::pow(kBucketRatio, static_cast<double>(bucket + 1));
}

double HistogramSnapshot::fractionAtMost(double ms) const {
    if (_totalCount <= 0) {
        return 1.0;
    }
    if (ms <= 0) {
        return 0.0;
    }
    std::size_t bucket = latencyBucketOf(ms);
    double below = bucket == 0 ? 0.0 : _cumulative[bucket - 1];
    double inBucket = _cumulative[bucket] - below;
    double lo = lowerEdgeMs(bucket);
    double hi = upperEdgeMs(bucket);
    double fraction = std::min(1.0, std::max(0.0, (ms - lo) / (hi - lo)));
    return std::min(1.0, (below + inBucket * fraction) / _totalCount);
}

double HistogramSnapshot::quantileMs(double q) const {
    if (_totalCount <= 0) {
        return 0.0;
    }
    double target = q * _totalCount;
    for (std::size_t b = 0; b < kLatencyBuckets; ++b) {
        if (_cumulative[b] >= target) {
            double below = b == 0 ? 0.0 : _cumulative[b - 1];
            double inBucket = _cumulative[b] - below;
            double fraction = inBucket <= 0 ? 0.0 : (target - below) / inBucket;
            return lowerEdgeMs(b) + (upperEdgeMs(b) - lowerEdgeMs(b)) * fraction;
        }
    }
    return upperEdgeMs(kLatencyBuckets - 1);
}

// ===== HistogramCell =====

const HistogramSnapshot HistogramCell::kEmptySnapshot{};

int HistogramCell::staleTicks(double decay) {
    return static_cast<int>(std::ceil(std::log(kStaleWeight) / std::log(decay)));
}

HistogramCell::HistogramCell() : _published(&kEmptySnapshot) {}

void HistogramCell::atomicAdd(std::atomic<double>& target, double delta) {
    double current = target.load(std::memory_order_relaxed);
    while (!target.compare_exchange_weak(
        current, current + delta, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

double HistogramCell::exchangeZero(std::atomic<double>& target) {
    return target.exchange(0.0, std::memory_order_relaxed);
}

void HistogramCell::file(double serviceMs, std::size_t stripeHint) {
    Stripe& stripe = _stripes[stripeHint % kStripes];
    atomicAdd(stripe.buckets[HistogramSnapshot::latencyBucketOf(serviceMs)], 1.0);
    atomicAdd(stripe.sum, serviceMs);
    atomicAdd(stripe.count, 1.0);
}

void HistogramCell::refreshTick(double decay) {
    std::array<double, HistogramSnapshot::kLatencyBuckets> cumulative{};
    double pendingSum = 0.0;
    double pendingCount = 0.0;
    std::array<double, HistogramSnapshot::kLatencyBuckets> pendingBuckets{};
    for (Stripe& stripe : _stripes) {
        for (std::size_t b = 0; b < HistogramSnapshot::kLatencyBuckets; ++b) {
            pendingBuckets[b] += exchangeZero(stripe.buckets[b]);
        }
        pendingSum += exchangeZero(stripe.sum);
        pendingCount += exchangeZero(stripe.count);
    }
    double running = 0.0;
    for (std::size_t b = 0; b < HistogramSnapshot::kLatencyBuckets; ++b) {
        _state[b] = (_state[b] + pendingBuckets[b]) * decay;
        running += _state[b];
        cumulative[b] = running;
    }
    _stateSum = (_stateSum + pendingSum) * decay;
    _stateCount = (_stateCount + pendingCount) * decay;
    const int stale = staleTicks(decay);
    _idleTicks = pendingCount > 0 ? 0 : std::min(_idleTicks + 1, stale);
    if (_idleTicks >= stale && _stateCount > 0) {
        _state.fill(0.0);
        cumulative.fill(0.0);
        _stateSum = 0.0;
        _stateCount = 0.0;
    }
    double mean = _stateCount <= 0 ? 0.0 : _stateSum / _stateCount;

    auto& slot = _generations[_nextGeneration % kSnapshotGenerations];
    ++_nextGeneration;
    slot = std::make_unique<HistogramSnapshot>(cumulative, _stateCount, mean);
    _published.store(slot.get(), std::memory_order_release);
}

// ===== HistogramTable =====

HistogramTable::HistogramTable(std::size_t numCells, double decay) : _decay(decay) {
    if (!(decay > 0) || !(decay < 1)) {
        throw std::invalid_argument("histogram decay must be in (0, 1)");
    }
    _cells.reserve(numCells);
    for (std::size_t i = 0; i < numCells; ++i) {
        _cells.push_back(std::make_unique<HistogramCell>());
    }
}

void HistogramTable::file(std::size_t cell, double serviceMs) {
    static thread_local const std::size_t stripe =
        std::hash<const void*>{}(static_cast<const void*>(&stripe)) % HistogramCell::kStripes;
    _cells.at(cell)->file(serviceMs, stripe);
}

void HistogramTable::refreshTick() {
    for (auto& cell : _cells) {
        cell->refreshTick(_decay);
    }
}

void HistogramTable::setDecay(double decay) {
    if (!(decay > 0) || !(decay < 1)) {
        throw std::invalid_argument("histogram decay must be in (0, 1)");
    }
    _decay = decay;
}

const HistogramSnapshot* HistogramTable::snapshot(std::size_t cell) const {
    return _cells.at(cell)->snapshot();
}

}  // namespace mongo::chameleon
