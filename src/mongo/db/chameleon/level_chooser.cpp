#include "mongo/db/chameleon/level_chooser.h"

#include <algorithm>
#include <cmath>

namespace mongo::chameleon {

const char* readLevelName(ReadLevel level) {
    switch (level) {
        case ReadLevel::kEventualLocal:
            return "EVENTUAL_LOCAL";
        case ReadLevel::kEventualMajority:
            return "EVENTUAL_MAJORITY";
        case ReadLevel::kCausalLocal:
            return "CAUSAL_LOCAL";
        case ReadLevel::kCausalMajority:
            return "CAUSAL_MAJORITY";
        case ReadLevel::kLinearizable:
            return "LINEARIZABLE";
    }
    return "UNKNOWN";
}

// ===== RiderCaps =====

RiderCaps::RiderCaps(std::size_t numCells) {
    _riders.reserve(numCells);
    for (std::size_t i = 0; i < numCells; ++i) {
        _riders.push_back(std::make_unique<std::atomic<int>>(0));
    }
}

bool RiderCaps::tryAcquire(std::size_t cell) {
    auto& counter = *_riders.at(cell);
    if (counter.fetch_add(1, std::memory_order_relaxed) + 1 <= kCap) {
        return true;
    }
    counter.fetch_sub(1, std::memory_order_relaxed);
    return false;
}

void RiderCaps::release(std::size_t cell) {
    _riders.at(cell)->fetch_sub(1, std::memory_order_relaxed);
}

// ===== Scoring =====

namespace {

/** F_wait shifted right by the exec mean: latency = exec + wait with exec treated as constant. */
double shiftedCdf(const HistogramSnapshot& wait, double execMeanMs, double x) {
    double shifted = x - execMeanMs;
    if (wait.empty()) {
        return shifted > 0 ? 1.0 : 0.0;
    }
    if (shifted <= 0) {
        return 0.0;
    }
    return wait.fractionAtMost(shifted);
}

/**
 * F_exec convolved with F_wait over the exec snapshot's buckets:
 * F(x) = sum_b p_exec(b) * F_wait(x - mid(b)), independence assumed.
 */
double convolvedCdf(const HistogramSnapshot& exec, const HistogramSnapshot& wait, double x) {
    double total = 0.0;
    for (std::size_t b = 0; b < HistogramSnapshot::kLatencyBuckets; ++b) {
        double lo = HistogramSnapshot::lowerEdgeMs(b);
        double hi = HistogramSnapshot::upperEdgeMs(b);
        double mass = exec.fractionAtMost(hi) - exec.fractionAtMost(lo);
        if (mass <= 0) {
            continue;
        }
        double mid = (lo + hi) / 2.0;
        total += mass * shiftedCdf(wait, mid, x);
        if (hi > x && mass > 0 && total >= 1.0) {
            break;
        }
    }
    return std::min(1.0, total);
}

}  // namespace

Candidate scoreCandidate(const ScoringInputs& in,
                         int strength,
                         std::size_t cell,
                         std::size_t gapBucket,
                         const HistogramSnapshot& waitSnapshot) {
    const bool useConvolution = in.execSnapshot != nullptr && !in.execSnapshot->empty();
    Cdf cdf = [&](double x) {
        return useConvolution ? convolvedCdf(*in.execSnapshot, waitSnapshot, x)
                              : shiftedCdf(waitSnapshot, in.execMeanMs, x);
    };
    double execMean = useConvolution ? in.execSnapshot->meanMs() : in.execMeanMs;
    double omega = execMean + waitSnapshot.meanMs();
    ScoredLevel scored = scoreLevel(*in.sla, strength, in.rhoMs, cdf, omega, in.lambda);
    return Candidate{strength, cell, gapBucket, scored, waitSnapshot.empty(), false};
}

std::optional<Candidate> choose(std::vector<Candidate>& candidates, RiderCaps& riders) {
    std::vector<bool> excluded(candidates.size(), false);
    while (true) {
        Candidate* best = nullptr;
        for (std::size_t i = 0; i < candidates.size(); ++i) {
            if (excluded[i]) {
                continue;
            }
            // Strictly greater: ties go to the earlier (weaker) candidate.
            if (best == nullptr || candidates[i].scored.value > best->scored.value) {
                best = &candidates[i];
            }
        }
        if (best == nullptr || best->scored.value <= 0) {
            return std::nullopt;
        }
        if (!best->uncalibrated) {
            return *best;
        }
        if (riders.tryAcquire(best->cell)) {
            Candidate chosen = *best;
            chosen.holdsRider = true;
            return chosen;
        }
        excluded[static_cast<std::size_t>(best - candidates.data())] = true;
    }
}

}  // namespace mongo::chameleon
