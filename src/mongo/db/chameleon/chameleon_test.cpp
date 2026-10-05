#include "mongo/db/chameleon/chameleon.h"
#include "mongo/db/chameleon/chameleon_request.h"
#include "mongo/db/chameleon/level_chooser.h"
#include "mongo/db/chameleon/price_controller.h"
#include "mongo/db/chameleon/rung_scorer.h"
#include "mongo/db/chameleon/service_time_histograms.h"

#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/unittest/unittest.h"

#include <cmath>

namespace mongo::chameleon {
namespace {

const std::vector<Rung> kWorkedExampleSla{{4, 150, 10}, {3, 150, 6}, {3, 400, 4}, {0, 400, 1}, {0, 150, 5}};

Cdf stepCdf(double below200, double above200) {
    return [=](double x) {
        return x < 200 ? below200 : above200;
    };
}

// DESIGN.md step 4 worked example: rho = 20, thresholds 130 and 380 server side.
TEST(ChameleonScorer, WorkedExampleExpectedProfit) {
    auto ev = scoreLevel(kWorkedExampleSla, 0, 20, stepCdf(0.99, 1.00), 2, 0.02);
    auto cm = scoreLevel(kWorkedExampleSla, 3, 20, stepCdf(0.90, 0.99), 40, 0.02);
    auto lin = scoreLevel(kWorkedExampleSla, 4, 20, stepCdf(0.70, 0.97), 80, 0.02);
    ASSERT_APPROX_EQUAL(ev.expectedProfit, 4.96, 1e-9);
    ASSERT_APPROX_EQUAL(cm.expectedProfit, 5.76, 1e-9);
    ASSERT_APPROX_EQUAL(lin.expectedProfit, 8.08, 1e-9);
    ASSERT_APPROX_EQUAL(ev.value, 4.92, 1e-9);
    ASSERT_APPROX_EQUAL(cm.value, 4.96, 1e-9);
    ASSERT_APPROX_EQUAL(lin.value, 6.48, 1e-9);
    ASSERT_APPROX_EQUAL(lin.dMaxMs, 380, 1e-9);
}

TEST(ChameleonScorer, WorkedExamplePriceFlipsTheChoice) {
    auto ev = scoreLevel(kWorkedExampleSla, 0, 20, stepCdf(0.99, 1.00), 2, 0.08);
    auto cm = scoreLevel(kWorkedExampleSla, 3, 20, stepCdf(0.90, 0.99), 40, 0.08);
    auto lin = scoreLevel(kWorkedExampleSla, 4, 20, stepCdf(0.70, 0.97), 80, 0.08);
    ASSERT_APPROX_EQUAL(ev.value, 4.80, 1e-9);
    ASSERT_APPROX_EQUAL(cm.value, 2.56, 1e-9);
    ASSERT_APPROX_EQUAL(lin.value, 1.68, 1e-9);
}

// The price is bounded on both sides: the floor lets it leave zero and the ceiling stops
// windup under sustained overload, so recovery takes log(lambdaMax) rather than never.
TEST(ChameleonPrice, FloorAndCeilingBoundTheMultiplicativeUpdate) {
    PriceController price(0.85, 0.1, 1e-4, 1e6);
    ASSERT_EQ(price.lambda(), 0.0);
    price.update(0.5);
    ASSERT_EQ(price.lambda(), 1e-4) << "the floor lifts the price off the absorbing zero";
    for (int i = 0; i < 100000; ++i) {
        price.update(18.74);  // the local topology's Flash utilization with in-flight pinned by the cap
    }
    ASSERT_EQ(price.lambda(), 1e6) << "sustained overload stops at the ceiling instead of overflowing";
    ASSERT_TRUE(std::isfinite(price.lambda()));
    int ticks = 0;
    while (price.lambda() > 1e-4 && ticks < 10000) {
        price.update(0.0);
        ++ticks;
    }
    ASSERT_EQ(price.lambda(), 1e-4);
    // log(1e6 / 1e-4) / (0.1 * 0.85) = 271 intervals: seconds at 100 ms, not the 460 s the
    // uncapped regional run needed from 1e167.
    ASSERT_LT(ticks, 300);
}

TEST(ChameleonPrice, ReconfigureMovesTheBounds) {
    PriceController price(0.85, 0.1, 1e-4, 1e6);
    price.forceLambda(500.0);
    price.reconfigure(0.85, 0.1, 1e-4, 100.0);
    price.update(0.85);
    ASSERT_EQ(price.lambda(), 100.0) << "a lowered ceiling clamps the very next update";
    ASSERT_EQ(price.lambdaMin(), 1e-4);
    ASSERT_EQ(price.lambdaMax(), 100.0);
}

TEST(ChameleonPrice, InvertedBoundsAreHeldAtTheCeilingNotFatal) {
    // The pair is validated by the driver's config loader; the two server parameters land one
    // at a time, so an inverted pair in between must leave a finite, deterministic price.
    PriceController price(0.85, 0.1, 10.0, 1.0);
    price.update(2.0);
    ASSERT_EQ(price.lambda(), 1.0);
}

TEST(ChameleonHistograms, DecayAndCdf) {
    HistogramTable table(CellIndex::numCells(3), 0.9);
    const std::size_t cell = CellIndex::read(ReadLevel::kCausalLocal, 1);
    ASSERT_TRUE(table.snapshot(cell)->empty());
    ASSERT_EQ(table.snapshot(cell)->fractionAtMost(5), 1.0);
    for (int i = 0; i < 1000; ++i) {
        table.file(cell, 10.0);
    }
    table.refreshTick();
    const HistogramSnapshot* snapshot = table.snapshot(cell);
    ASSERT_APPROX_EQUAL(snapshot->totalCount(), 900, 1e-6);
    ASSERT_APPROX_EQUAL(snapshot->meanMs(), 10.0, 1e-9);
    ASSERT_APPROX_EQUAL(snapshot->fractionAtMost(20), 1.0, 1e-9);
    ASSERT_APPROX_EQUAL(snapshot->fractionAtMost(5), 0.0, 1e-9);
    table.refreshTick();
    ASSERT_APPROX_EQUAL(table.snapshot(cell)->totalCount(), 810, 1e-6);
}

TEST(ChameleonHistograms, ACellNobodySamplesIsForgottenOnceItsNewestSampleWeighsOnePercent) {
    // A level that looked bad once and then stopped being chosen: decay alone
    // would keep the bad shape forever, since the scorer only samples what it picks.
    HistogramTable table(CellIndex::numCells(3), 0.95);
    const std::size_t cell = CellIndex::read(ReadLevel::kLinearizable, 0);
    const int stale = HistogramCell::staleTicks(0.95);
    ASSERT_EQ(stale, 90) << "0.95^90 is the first power below 1%";
    for (int i = 0; i < 100; ++i) {
        table.file(cell, 400.0);
    }
    table.refreshTick();
    ASSERT_APPROX_EQUAL(table.snapshot(cell)->fractionAtMost(200), 0.0, 1e-9);

    for (int idle = 1; idle < stale; ++idle) {
        table.refreshTick();
        ASSERT_GT(table.snapshot(cell)->totalCount(), 0) << "still remembered after " << idle << " idle ticks";
        ASSERT_APPROX_EQUAL(table.snapshot(cell)->fractionAtMost(200), 0.0, 1e-9);
    }
    table.refreshTick();
    const HistogramSnapshot* snapshot = table.snapshot(cell);
    ASSERT_TRUE(snapshot->empty()) << "forgotten after staleTicks idle ticks";
    ASSERT_EQ(snapshot->fractionAtMost(200), 1.0) << "forgotten, the cell is uncalibrated again";
    ASSERT_EQ(snapshot->meanMs(), 0.0);
}

TEST(ChameleonHistograms, ACellThatKeepsBeingSampledIsNeverForgotten) {
    // One sample every 60 ticks: the decayed count drops well below one
    // sample between them, but the evidence is fresh, so it is kept.
    HistogramTable table(CellIndex::numCells(3), 0.95);
    const std::size_t cell = CellIndex::read(ReadLevel::kLinearizable, 0);
    for (int tick = 0; tick < 600; ++tick) {
        if (tick % 60 == 0) {
            table.file(cell, 10.0);
        }
        table.refreshTick();
        ASSERT_GT(table.snapshot(cell)->totalCount(), 0) << "a sampled cell must keep its evidence, tick " << tick;
    }
    ASSERT_APPROX_EQUAL(table.snapshot(cell)->meanMs(), 10.0, 1e-9);
}

TEST(ChameleonChooser, RiderCapFallsToNextLevel) {
    RiderCaps riders(CellIndex::numCells(kMaxWriteConcern));
    HistogramSnapshot empty;
    ScoringInputs in{&kWorkedExampleSla, 20, 0.0};
    std::vector<Candidate> candidates;
    for (int level = 0; level < kReadLevels; ++level) {
        auto cell = CellIndex::read(static_cast<ReadLevel>(level), 0);
        candidates.push_back(scoreCandidate(in, level, cell, 0, empty));
    }
    auto chosen = choose(candidates, riders);
    ASSERT_TRUE(chosen.has_value());
    ASSERT_EQ(chosen->strength, 4);
    ASSERT_TRUE(chosen->holdsRider);
    for (int i = 0; i < RiderCaps::kCap - 1; ++i) {
        ASSERT_TRUE(riders.tryAcquire(CellIndex::read(ReadLevel::kLinearizable, 0)));
    }
    for (auto& candidate : candidates) {
        candidate.holdsRider = false;
    }
    auto next = choose(candidates, riders);
    ASSERT_TRUE(next.has_value());
    ASSERT_EQ(next->strength, 3);
}

TEST(ChameleonChooser, ExecutionEstimateShiftsThresholds) {
    HistogramSnapshot empty;
    ScoringInputs shifted{&kWorkedExampleSla, 20, 0.0, 200.0};
    auto candidate = scoreCandidate(shifted, 4, 0, 0, empty);
    // Only the 380 ms threshold survives a 200 ms execution estimate.
    ASSERT_APPROX_EQUAL(candidate.scored.expectedProfit, 4.0, 1e-9);
    ScoringInputs pricey{&kWorkedExampleSla, 20, 1000.0, 5.0};
    RiderCaps riders(CellIndex::numCells(kMaxWriteConcern));
    std::vector<Candidate> only{scoreCandidate(pricey, 0, 0, 0, empty)};
    ASSERT_FALSE(choose(only, riders).has_value());
}

TEST(ChameleonSla, ParseAndSerializeRoundTrip) {
    BSONObj doc = BSON(
        "read" << BSON_ARRAY(BSON("app" << 1 << "sla" << 2 << "rungs"
                                        << BSON_ARRAY(BSON("strength" << 3 << "latencyMs" << 60 << "profit" << 50)
                                                      << BSON("strength" << 0 << "latencyMs" << 60 << "profit" << 15))))
               << "write"
               << BSON_ARRAY(BSON("app" << 1 << "sla" << 1 << "rungs"
                                        << BSON_ARRAY(BSON("strength" << 3 << "latencyMs" << 200 << "profit" << 12)))));
    SlaRegistry::Tables tables;
    ASSERT_OK(parseSlaTables(doc, &tables));
    ASSERT_EQ(tables.read.size(), 1u);
    ASSERT_EQ(tables.write.size(), 1u);
    const auto* read = SlaRegistry::find(tables.read, 1, 2);
    ASSERT_TRUE(read != nullptr);
    ASSERT_EQ(read->size(), 2u);
    ASSERT_EQ((*read)[0].strength, 3);
    ASSERT_APPROX_EQUAL((*read)[1].profit, 15.0, 1e-12);
    ASSERT_TRUE(SlaRegistry::find(tables.write, 1, 2) == nullptr);

    SlaRegistry::Tables again;
    ASSERT_OK(parseSlaTables(serializeSlaTables(tables), &again));
    ASSERT_EQ(again.read.size(), 1u);
    ASSERT_EQ(again.write.size(), 1u);
}

TEST(ChameleonSla, ParseRejectsMalformedTable) {
    SlaRegistry::Tables tables;
    ASSERT_NOT_OK(parseSlaTables(BSON("read" << 5), &tables));
    ASSERT_NOT_OK(parseSlaTables(BSON("read" << BSON_ARRAY(BSON("app" << 1 << "sla" << 1))), &tables));
    ASSERT_NOT_OK(parseSlaTables(
        BSON("read" << BSON_ARRAY(BSON("app" << 1 << "sla" << 1 << "rungs"
                                             << BSON_ARRAY(BSON("strength" << 1 << "latencyMs" << -5 << "profit" << 1))))),
        &tables));
    ASSERT_NOT_OK(parseSlaTables(
        BSON("read" << BSON_ARRAY(BSON("app" << 1 << "sla" << 1 << "rungs"
                                             << BSON_ARRAY(BSON("strength" << 1 << "latencyMs" << 5 << "profit" << 1)))
                                  << BSON("app" << 1 << "sla" << 1 << "rungs"
                                                << BSON_ARRAY(BSON("strength" << 1 << "latencyMs" << 5 << "profit" << 1))))),
        &tables));
}

TEST(ChameleonEnvelope, ParsesCommentEnvelope) {
    BSONObj comment = BSON("comment" << BSON("app" << 2 << "sla" << 1 << "rtt" << 31.5 << "read" << true
                                                   << "frontier" << Timestamp(1700000000, 42) << "target" << 3
                                                   << "wantLin" << false << "wc" << 0 << "waitBound" << 120.0));
    auto envelope = parseEnvelope(boost::optional<BSONElement>(comment.firstElement()));
    ASSERT_TRUE(envelope.has_value());
    ASSERT_EQ(envelope->applicationId, 2);
    ASSERT_EQ(envelope->slaId, 1);
    ASSERT_APPROX_EQUAL(envelope->rttMs, 31.5, 1e-12);
    ASSERT_TRUE(envelope->isRead);
    ASSERT_TRUE(envelope->frontier.has_value());
    ASSERT_EQ(envelope->frontier->getSecs(), 1700000000u);
    ASSERT_EQ(envelope->frontier->getInc(), 42u);
    ASSERT_TRUE(envelope->targetLevel.has_value());
    ASSERT_EQ(*envelope->targetLevel, 3);
    ASSERT_APPROX_EQUAL(envelope->waitBoundMs, 120.0, 1e-12);
}

TEST(ChameleonEnvelope, IgnoresOrdinaryComments) {
    BSONObj plain = BSON("comment" << "hello");
    ASSERT_FALSE(parseEnvelope(boost::optional<BSONElement>(plain.firstElement())).has_value());
    BSONObj other = BSON("comment" << BSON("purpose" << "audit"));
    ASSERT_FALSE(parseEnvelope(boost::optional<BSONElement>(other.firstElement())).has_value());
    ASSERT_FALSE(parseEnvelope(boost::none).has_value());
}

TEST(ChameleonEnvelope, RejectsMalformedEnvelope) {
    BSONObj bad = BSON("comment" << BSON("app" << "one" << "sla" << 1 << "read" << true));
    ASSERT_THROWS_CODE(parseEnvelope(boost::optional<BSONElement>(bad.firstElement())),
                       DBException,
                       ErrorCodes::BadValue);
}

TEST(ChameleonShape, LiteralsDoNotChangeTheKey) {
    BSONObj a = BSON("find" << "kv" << "filter" << BSON("_id" << "user1") << "limit" << 1);
    BSONObj b = BSON("find" << "kv" << "filter" << BSON("_id" << "user999") << "limit" << 1);
    BSONObj c = BSON("find" << "kv" << "filter" << BSON("category" << "x" << "price" << BSON("$lt" << 5))
                            << "sort" << BSON("price" << 1));
    ASSERT_EQ(shapeKeyOf(a), shapeKeyOf(b));
    ASSERT_NE(shapeKeyOf(a), shapeKeyOf(c));
    BSONObj u1 = BSON("update" << "kv" << "updates"
                               << BSON_ARRAY(BSON("q" << BSON("_id" << "k") << "u" << BSON("$set" << BSON("v" << 1))
                                                      << "multi" << false)));
    BSONObj u2 = BSON("update" << "kv" << "updates"
                               << BSON_ARRAY(BSON("q" << BSON("_id" << "k") << "u" << BSON("$set" << BSON("v" << 1))
                                                      << "multi" << true)));
    ASSERT_NE(shapeKeyOf(u1), shapeKeyOf(u2));
    ASSERT_NE(shapeKeyOf(a), shapeKeyOf(u1));
}

}  // namespace
}  // namespace mongo::chameleon
