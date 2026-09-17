#include "mongo/db/chameleon/rung_scorer.h"

#include <algorithm>
#include <utility>

namespace mongo::chameleon {

ScoredLevel scoreLevel(const std::vector<Rung>& slaRungs,
                       int levelStrength,
                       double rhoMs,
                       const Cdf& cdf,
                       double omegaMs,
                       double lambda) {
    // 4a: restrict to satisfiable rungs.
    std::size_t satisfiable = 0;
    // 4b: convert thresholds to the server's clock; discard spent budgets.
    std::vector<std::pair<double, double>> survivors;  // (d, profit)
    survivors.reserve(slaRungs.size());
    for (const Rung& rung : slaRungs) {
        if (levelStrength < rung.strength) {
            continue;
        }
        ++satisfiable;
        double d = rung.thresholdMs - rhoMs;
        if (d > 0) {
            survivors.emplace_back(d, rung.profit);
        }
    }
    if (survivors.empty()) {
        return ScoredLevel{0.0, -lambda * omegaMs, 0.0, satisfiable, 0};
    }

    // 4c: sort ascending by threshold and collapse identical thresholds.
    std::sort(survivors.begin(), survivors.end(), [](const auto& a, const auto& b) {
        return a.first < b.first;
    });
    // 4d: suffix maximum of profits, walking from the loosest threshold down.
    // thresholds/suffixMax end up DESCENDING.
    std::vector<double> thresholds;
    std::vector<double> suffixMax;
    thresholds.reserve(survivors.size());
    suffixMax.reserve(survivors.size());
    for (std::size_t i = survivors.size(); i-- > 0;) {
        double d = survivors[i].first;
        double profit = survivors[i].second;
        if (!thresholds.empty() && thresholds.back() == d) {
            suffixMax.back() = std::max(suffixMax.back(), profit);
        } else {
            thresholds.push_back(d);
            suffixMax.push_back(suffixMax.empty() ? profit : std::max(suffixMax.back(), profit));
        }
    }

    // 4e: by-parts expected profit, walked back to ascending order.
    double expected = 0.0;
    for (std::size_t i = thresholds.size(); i-- > 0;) {
        double mNext = (i > 0) ? suffixMax[i - 1] : 0.0;  // M_(i+1) in ascending order
        expected += (suffixMax[i] - mNext) * cdf(thresholds[i]);
    }

    // 4f: pay for the capacity the level consumes.
    double value = expected - lambda * omegaMs;
    return ScoredLevel{expected, value, thresholds.front(), satisfiable, survivors.size()};
}

}  // namespace mongo::chameleon
