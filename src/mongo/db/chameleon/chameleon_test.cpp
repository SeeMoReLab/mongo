#include "mongo/db/chameleon/chameleon.h"
#include "mongo/db/chameleon/chameleon_request.h"
#include "mongo/db/chameleon/level_chooser.h"
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
