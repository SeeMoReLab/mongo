/**
 * DESIGN.md steps 3 to 5 for one request: score every legal level against
 * the request's SLA from the histogram cells, pick the best, and apply the
 * cold-start rider cap. Standard library only; the caller supplies the cell
 * lookup and the gap per level so this stays independent of mongod.
 *
 * Cell layout (shared with the Java store): read levels occupy cells
 * level * kGapBuckets + gapBucket for level in 0..4; write concern w occupies
 * cell (5 + w - 1) * kGapBuckets + 0.
 */
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "mongo/db/chameleon/rung_scorer.h"
#include "mongo/db/chameleon/service_time_histograms.h"

namespace mongo::chameleon {

/** Read levels, in strength order; the numbering matches the Java ReadLevel enum. */
enum class ReadLevel : int {
    kEventualLocal = 0,
    kEventualMajority = 1,
    kCausalLocal = 2,
    kCausalMajority = 3,
    kLinearizable = 4,
};

constexpr int kReadLevels = 5;

const char* readLevelName(ReadLevel level);

/** Maps (level or write concern, gap bucket) to a cell of the wait-histogram table. */
struct CellIndex {
    static std::size_t numCells(int majority) {
        return static_cast<std::size_t>(kReadLevels + majority) * GapBuckets::kCount;
    }
    static std::size_t read(ReadLevel level, std::size_t gapBucket) {
        return static_cast<std::size_t>(static_cast<int>(level)) * GapBuckets::kCount + gapBucket;
    }
    static std::size_t write(int writeConcern) {
        return static_cast<std::size_t>(kReadLevels + writeConcern - 1) * GapBuckets::kCount;
    }
};

/**
 * Cold-start rider counters, one per cell: a level whose cell has no samples
 * is treated as free and certain, so at most kCap requests may ride an
 * uncalibrated cell concurrently (samples arrive only on completion).
 */
class RiderCaps {
public:
    static constexpr int kCap = 64;

    explicit RiderCaps(std::size_t numCells);

    bool tryAcquire(std::size_t cell);
    void release(std::size_t cell);

private:
    std::vector<std::unique_ptr<std::atomic<int>>> _riders;
};

/** One scored candidate. */
struct Candidate {
    int strength;           // ReadLevel ordinal for reads, write concern for writes
    std::size_t cell;       // wait-histogram cell scored
    std::size_t gapBucket;
    ScoredLevel scored;
    bool uncalibrated;      // the wait cell had no samples
    bool holdsRider = false;
};

/** Everything a request needs to score: SLA, rho, lambda, and the exec term. */
struct ScoringInputs {
    const std::vector<Rung>* sla;
    double rhoMs;
    double lambda;
    /** Execution-time estimate common to every level (0 when not estimated). */
    double execMeanMs = 0.0;
    /** F_exec(x) for the shape, or nullptr to treat exec as the constant execMeanMs. */
    const HistogramSnapshot* execSnapshot = nullptr;
};

/**
 * Score one level from its wait cell. The request's latency under the level
 * is exec + wait, so the CDF used is F_wait shifted right by the exec mean
 * (or convolved with F_exec when a snapshot is supplied) and omega is the sum
 * of the two means.
 */
Candidate scoreCandidate(const ScoringInputs& in,
                         int strength,
                         std::size_t cell,
                         std::size_t gapBucket,
                         const HistogramSnapshot& waitSnapshot);

/**
 * Step 4g plus the rider cap: the highest score wins, ties go to the weakest
 * level (an upgrade must be strictly better), and a level whose cell is
 * uncalibrated is only eligible while its rider cap has room. Returns
 * nullopt when nothing scores above zero or every positive candidate is
 * excluded (step 5 rejection). On success the returned candidate may hold a
 * rider, which the caller must release on completion.
 */
std::optional<Candidate> choose(std::vector<Candidate>& candidates, RiderCaps& riders);

}  // namespace mongo::chameleon
