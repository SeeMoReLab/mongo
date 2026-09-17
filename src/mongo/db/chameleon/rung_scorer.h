/**
 * Chameleon step 4: score one candidate consistency level against an SLA's
 * rungs. Pure functions over (rungs, level strength, rho, F_c, omega_c,
 * lambda), unit-testable against DESIGN.md's worked example. Standard library
 * only: this header is shared by the mongod glue and by standalone tests.
 */
#pragma once

#include <cstddef>
#include <functional>
#include <vector>

namespace mongo::chameleon {

/**
 * One SLA rung (kappa, delta, pi): consistency requirement as a comparable
 * strength, end-to-end latency threshold in milliseconds, and profit. Read
 * rungs use the ReadLevel ordinal as strength; write rungs use the write
 * concern (acknowledgements to wait for).
 */
struct Rung {
    int strength;
    double thresholdMs;
    double profit;
};

/** Result of scoring one level. */
struct ScoredLevel {
    /** E_c: expected profit of running this level. */
    double expectedProfit;
    /** V_c = E_c - lambda * omega_c. */
    double value;
    /** Loosest surviving server-side threshold; bounds the step 6 wait. 0 if none survive. */
    double dMaxMs;
    /** Rungs this level satisfies before threshold conversion (4a). */
    std::size_t satisfiableRungs;
    /** Rungs remaining after discarding thresholds the network consumed (4b). */
    std::size_t survivingRungs;
};

/** F_c(x): fraction of recent executions of the level that finished within x ms. */
using Cdf = std::function<double(double)>;

/**
 * Score level c. 'cdf' is F_c of the histogram cell this request would run
 * under; 'omegaMs' is that cell's running mean (the occupancy cost).
 *
 * 4a restrict to satisfiable rungs; 4b move thresholds to the server's clock
 * by subtracting rho and drop spent budgets; 4c sort ascending and collapse
 * identical thresholds; 4d suffix maximum of profits; 4e by-parts expected
 * profit E_c = sum_i (M_i - M_(i+1)) F_c(d_(i)); 4f V_c = E_c - lambda omega_c.
 */
ScoredLevel scoreLevel(const std::vector<Rung>& slaRungs,
                       int levelStrength,
                       double rhoMs,
                       const Cdf& cdf,
                       double omegaMs,
                       double lambda);

}  // namespace mongo::chameleon
