/**
 * Shadow price controller (DESIGN.md background tasks): once per control
 * interval, lambda <- max(lambdaMin, lambda * exp(eta * (u - uTarget))).
 * Multiplicative so the price spans orders of magnitude and never goes
 * negative; the floor lets it start from zero and recover after idle periods.
 * Requests read lambda, which is up to one interval stale by design.
 */
#pragma once

#include <algorithm>
#include <atomic>
#include <cmath>

namespace mongo::chameleon {

class PriceController {
public:
    PriceController(double uTarget, double eta, double lambdaMin)
        : _uTarget(uTarget), _eta(eta), _lambdaMin(lambdaMin) {}

    /** Called once per control interval with that interval's utilization. */
    void update(double utilization) {
        double current = _lambda.load(std::memory_order_relaxed);
        double next = std::max(_lambdaMin, current * std::exp(_eta * (utilization - _uTarget)));
        _lambda.store(next, std::memory_order_relaxed);
    }

    /** The published price. */
    double lambda() const {
        return _lambda.load(std::memory_order_relaxed);
    }

    /** Test-only: set the price directly to probe admission at a known lambda. */
    void forceLambda(double value) {
        _lambda.store(value, std::memory_order_relaxed);
    }

    void reconfigure(double uTarget, double eta, double lambdaMin) {
        _uTarget = uTarget;
        _eta = eta;
        _lambdaMin = lambdaMin;
    }

private:
    double _uTarget;
    double _eta;
    double _lambdaMin;
    std::atomic<double> _lambda{0.0};
};

}  // namespace mongo::chameleon
