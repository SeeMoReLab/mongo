/**
 * Decayed service-time histograms (DESIGN.md steps 3, 4 and 8 and the
 * histogram refresh task). A table of cells; the caller maps whatever it
 * keys on (a (level, gap bucket) pair, a query shape) to a cell index.
 *
 * Hot path: file() adds a sample into striped pending counters without
 * locking against readers. Refresh thread: refreshTick() folds the pending
 * samples into the decayed state and publishes a fresh immutable Snapshot per
 * cell. Requests read the published snapshot and never block on the refresh.
 *
 * Snapshot lifetime: publication keeps the last kSnapshotGenerations
 * snapshots alive, so a pointer obtained from snapshot() stays valid for at
 * least (kSnapshotGenerations - 1) refresh ticks after it was read. Requests
 * use a snapshot for microseconds; ticks are 100 ms apart.
 *
 * Decay forgets only while samples arrive: it rescales every bucket alike, so
 * a cell nobody samples keeps its shape, and the scorer samples only the level
 * it chooses (and nothing for a request it rejects). A level that once looked
 * bad would never be tried again. So a cell is emptied once even its newest
 * sample has decayed to kStaleWeight of its weight (90 ticks, about 9 s, at
 * 0.95): it is uncalibrated again, and the cold-start rider cap probes it. A
 * cell that keeps receiving samples, however rarely, keeps its evidence. The
 * Java store's ServiceTimeHistograms applies the same rule.
 *
 * Standard library only.
 */
#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

namespace mongo::chameleon {

class HistogramCell;

/** Immutable published view of one cell. */
class HistogramSnapshot {
public:
    static constexpr std::size_t kLatencyBuckets = 64;
    static constexpr double kMinLatencyMs = 0.5;
    static constexpr double kBucketRatio = 1.15;

    /** Latency bucket b covers [lowerEdgeMs(b), upperEdgeMs(b)); bucket 0 starts at 0. */
    static std::size_t latencyBucketOf(double ms);
    static double lowerEdgeMs(std::size_t bucket);
    static double upperEdgeMs(std::size_t bucket);

    HistogramSnapshot() = default;
    HistogramSnapshot(std::array<double, kLatencyBuckets> cumulative, double totalCount, double meanMs)
        : _cumulative(cumulative), _totalCount(totalCount), _meanMs(meanMs) {}

    /** Decayed sample count; 0 means the cell is uncalibrated. */
    double totalCount() const {
        return _totalCount;
    }

    /** Mean service time (omega, the occupancy cost); 0 when empty. */
    double meanMs() const {
        return _meanMs;
    }

    bool empty() const {
        return _totalCount <= 0;
    }

    /**
     * F(x): fraction of recent samples that finished within x ms, with linear
     * interpolation inside the bucket containing x. An empty cell is
     * optimistically certain (1.0): the uncalibrated level is treated as
     * free until samples arrive (step 4 cold-start rule).
     */
    double fractionAtMost(double ms) const;

    /** Approximate quantile in ms (interpolated); 0 when the cell is empty. */
    double quantileMs(double q) const;

private:
    std::array<double, kLatencyBuckets> _cumulative{};
    double _totalCount = 0.0;
    double _meanMs = 0.0;
};

/** One histogram cell: striped pending samples plus the refresh thread's decayed state. */
class HistogramCell {
public:
    static constexpr std::size_t kStripes = 16;
    static constexpr std::size_t kSnapshotGenerations = 8;
    // A cell is forgotten once its newest sample weighs this little.
    static constexpr double kStaleWeight = 0.01;

    /** Ticks without a sample after which a cell is emptied: the first n with decay^n <= kStaleWeight. */
    static int staleTicks(double decay);

    HistogramCell();
    HistogramCell(const HistogramCell&) = delete;
    HistogramCell& operator=(const HistogramCell&) = delete;

    /** File one sample: three additions, no rebuild. Safe from any thread. */
    void file(double serviceMs, std::size_t stripeHint);

    /** Fold pending samples into the decayed state and publish. Refresh thread only. */
    void refreshTick(double decay);

    /** The published snapshot; see the lifetime note in the file header. */
    const HistogramSnapshot* snapshot() const {
        return _published.load(std::memory_order_acquire);
    }

private:
    struct alignas(64) Stripe {
        std::array<std::atomic<double>, HistogramSnapshot::kLatencyBuckets> buckets{};
        std::atomic<double> sum{0.0};
        std::atomic<double> count{0.0};
    };

    static void atomicAdd(std::atomic<double>& target, double delta);
    static double exchangeZero(std::atomic<double>& target);

    std::array<Stripe, kStripes> _stripes;
    // Refresh thread state.
    std::array<double, HistogramSnapshot::kLatencyBuckets> _state{};
    double _stateSum = 0.0;
    double _stateCount = 0.0;
    int _idleTicks = 0;
    std::array<std::unique_ptr<HistogramSnapshot>, kSnapshotGenerations> _generations;
    std::size_t _nextGeneration = 0;
    std::atomic<const HistogramSnapshot*> _published;
    static const HistogramSnapshot kEmptySnapshot;
};

/** A fixed-size table of cells indexed by an integer key. */
class HistogramTable {
public:
    HistogramTable(std::size_t numCells, double decay);

    std::size_t numCells() const {
        return _cells.size();
    }

    void file(std::size_t cell, double serviceMs);
    void refreshTick();
    const HistogramSnapshot* snapshot(std::size_t cell) const;

    /** Change the per-tick decay; takes effect at the next refreshTick. Refresh thread only. */
    void setDecay(double decay);
    double decay() const {
        return _decay;
    }

private:
    double _decay;
    std::vector<std::unique_ptr<HistogramCell>> _cells;
};

/**
 * Coarse gap buckets (step 3). Gap is the lag the level must close before it
 * can be served; everything at or below zero lands in bucket 0, whose observed
 * waits are near zero, so "the upgrade is free" emerges from data. The two
 * inner edges are in the gap's own unit (milliseconds of lag in the MongoDB
 * port).
 */
class GapBuckets {
public:
    static constexpr std::size_t kCount = 4;

    GapBuckets(double edge1, double edge2) : _edge1(edge1), _edge2(edge2) {}

    std::size_t bucketOf(double gap) const {
        if (gap <= 0) {
            return 0;
        }
        if (gap <= _edge1) {
            return 1;
        }
        if (gap <= _edge2) {
            return 2;
        }
        return 3;
    }

private:
    double _edge1;
    double _edge2;
};

}  // namespace mongo::chameleon
