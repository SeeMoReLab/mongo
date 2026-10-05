/**
 * Shadow price controller (DESIGN.md background tasks): once per control
 * interval, lambda <- min(lambdaMax, max(lambdaMin, lambda * exp(eta * (u - uTarget)))).
 * Multiplicative so the price spans orders of magnitude and never goes
 * negative; the floor lets it start from zero and recover after idle periods.
 * Requests read lambda, which is up to one interval stale by design.
 *
 * The ceiling is anti-windup, the same one the Java store's PriceController
 * has. Under overload in-flight is pinned by the hard cap, not by the price,
 * so u stays above target no matter how high lambda climbs and the
 * multiplicative update compounds unopposed; without a ceiling the 31-minute
 * day sequence took lambda to 1e271 on the global topology, a factor of ~1e37
 * short of overflowing to infinity, from which no decay returns. A level is
 * refused once lambda exceeds max_rung_profit / omega, so nothing above that
 * changes a decision, and the ceiling bounds recovery to log(lambdaMax) /
 * (eta * uTarget) intervals.
 *
 * The pair is validated where it is authored (ExperimentConfig requires
 * lambdaMax > lambdaMin before the driver pushes either); here the two are
 * independent server parameters set one at a time, so a transiently inverted
 * pair must not be fatal. When lambdaMax <= lambdaMin the ceiling wins:
 * lambda is held at lambdaMax, and serverStatus reports both values so the
 * inversion is visible.
 */
#pragma once

#include <algorithm>
#include <atomic>
#include <cmath>

namespace mongo::chameleon {

class PriceController {
public:
    PriceController(double uTarget, double eta, double lambdaMin, double lambdaMax)
        : _uTarget(uTarget), _eta(eta), _lambdaMin(lambdaMin), _lambdaMax(lambdaMax) {}

    /** Called once per control interval with that interval's utilization. */
    void update(double utilization) {
        double current = _lambda.load(std::memory_order_relaxed);
        double raw = current * std::exp(_eta * (utilization - _uTarget));
        double next = std::min(_lambdaMax, std::max(_lambdaMin, raw));
        _lambda.store(next, std::memory_order_relaxed);
    }

    /** The published price. */
    double lambda() const {
        return _lambda.load(std::memory_order_relaxed);
    }

    double lambdaMin() const {
        return _lambdaMin;
    }

    double lambdaMax() const {
        return _lambdaMax;
    }

    /** Test-only: set the price directly to probe admission at a known lambda. */
    void forceLambda(double value) {
        _lambda.store(value, std::memory_order_relaxed);
    }

    void reconfigure(double uTarget, double eta, double lambdaMin, double lambdaMax) {
        _uTarget = uTarget;
        _eta = eta;
        _lambdaMin = lambdaMin;
        _lambdaMax = lambdaMax;
    }

private:
    double _uTarget;
    double _eta;
    double _lambdaMin;
    double _lambdaMax;
    std::atomic<double> _lambda{0.0};
};

}  // namespace mongo::chameleon
