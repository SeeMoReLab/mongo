/**
 * The Chameleon measurement plane and decision state for one mongod
 * (MONGODB_PORT.md, Milestones 1 and 2). Owns the registered SLAs, the wait
 * and execution histograms, the occupancy meter, the shadow price, the
 * replication bucket and the cold-start rider caps, runs the control tick
 * on the periodic runner, and publishes a "chameleon" serverStatus section.
 *
 * Per-request logic lives in chameleon_request.h; this class is the shared
 * state it reads and updates.
 */
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/chameleon/level_chooser.h"
#include "mongo/db/chameleon/occupancy_meter.h"
#include "mongo/db/chameleon/price_controller.h"
#include "mongo/db/chameleon/replication_rate_bucket.h"
#include "mongo/db/chameleon/service_time_histograms.h"
#include "mongo/db/chameleon/sla_registry.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/service_context.h"
#include "mongo/util/periodic_runner.h"

namespace mongo::chameleon {

/** Largest write concern the wait-histogram table has cells for (majority of up to 13 members). */
constexpr int kMaxWriteConcern = 7;

/** Reasons a request was shed, for the serverStatus counters. */
enum class RejectReason { kScorer, kReplicationBudget, kOccupancyCap };

/**
 * Process-wide SLA registry. Separate from the Chameleon instance so the
 * chameleonSlaTable parameter can be set before the instance exists.
 */
SlaRegistry& globalSlaRegistry();

/** Parse the chameleonSlaTable document into tables; explicit error on any malformed field. */
Status parseSlaTables(const BSONObj& doc, SlaRegistry::Tables* out);

/** Serialize tables back into the parameter's document shape. */
BSONObj serializeSlaTables(const SlaRegistry::Tables& tables);

class Chameleon {
public:
    static Chameleon* get(ServiceContext* service);
    static Chameleon* get(OperationContext* opCtx);
    static void set(ServiceContext* service, std::unique_ptr<Chameleon> instance);
    static void shutdown(ServiceContext* service);

    explicit Chameleon(ServiceContext* service);
    ~Chameleon();

    Chameleon(const Chameleon&) = delete;
    Chameleon& operator=(const Chameleon&) = delete;

    // ===== Shared state read on the request path =====

    HistogramTable& waitHistograms() {
        return _waitHistograms;
    }
    RiderCaps& riders() {
        return _riders;
    }
    OccupancyMeter& occupancy() {
        return _occupancy;
    }
    ReplicationRateBucket& replicationBucket() {
        return _replicationBucket;
    }
    double lambda() const {
        return _price.lambda();
    }
    /** Write majority of the current replica set config, refreshed every tick. */
    int majority() const {
        return _majority.load(std::memory_order_relaxed);
    }
    GapBuckets gapBuckets() const;

    /**
     * Milliseconds of lag a node whose view stands at 'target' must close to
     * cover 'frontier'. Zero or negative when the view already covers it. The
     * frontier is a hybrid logical clock value whose seconds are wall seconds
     * and whose increment counts oplog entries within that second; the
     * increment is converted to time with the entry rate measured per tick.
     */
    double gapMs(Timestamp frontier, const repl::OpTimeAndWallTime& target) const;

    // ===== Execution histograms keyed by query shape (Milestone 5) =====

    /** The shape's published snapshot; the shared overflow cell once the table is full. */
    const HistogramSnapshot* execSnapshot(uint64_t shapeKey);
    void fileExec(uint64_t shapeKey, double execMs);
    std::size_t execShapeCount() const;

    // ===== Counters =====

    void recordAdmitted() {
        _admitted.fetch_add(1, std::memory_order_relaxed);
    }
    void recordCompleted() {
        _completed.fetch_add(1, std::memory_order_relaxed);
    }
    void recordRejected(RejectReason reason);

    /** The serverStatus "chameleon" section. */
    BSONObj generateSection() const;

    /** One control tick: refresh histograms, close the occupancy interval, move the price. */
    void tick();

private:
    static constexpr std::size_t kMaxExecShapes = 4096;

    struct ExecCells {
        mutable std::shared_mutex mutex;
        std::unordered_map<uint64_t, std::unique_ptr<HistogramCell>> cells;
        HistogramCell overflow;
    };

    void refreshMajorityAndRate();

    ServiceContext* const _service;
    HistogramTable _waitHistograms;
    RiderCaps _riders;
    OccupancyMeter _occupancy;
    PriceController _price;
    ReplicationRateBucket _replicationBucket;
    ExecCells _exec;

    std::atomic<int> _majority{1};
    // Oplog entries per second at this node's lastApplied, EMA over ticks.
    std::atomic<double> _entriesPerSecond{1000.0};
    Timestamp _lastAppliedSample;
    int64_t _lastAppliedSampleNanos = 0;

    std::atomic<double> _lastUtilization{0.0};
    std::atomic<double> _lastAverageInFlight{0.0};
    std::atomic<int> _lastInFlightAtClose{0};
    std::atomic<double> _configuredBudget{0.0};

    std::atomic<int64_t> _admitted{0};
    std::atomic<int64_t> _completed{0};
    std::atomic<int64_t> _rejectedScorer{0};
    std::atomic<int64_t> _rejectedReplication{0};
    std::atomic<int64_t> _rejectedOccupancy{0};

    PeriodicRunner::JobAnchor _jobAnchor;
};

}  // namespace mongo::chameleon
