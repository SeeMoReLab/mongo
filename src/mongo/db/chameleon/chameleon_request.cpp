#include "mongo/db/chameleon/chameleon_request.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <string>
#include <vector>

#include "mongo/base/error_codes.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/chameleon/chameleon.h"
#include "mongo/db/chameleon/chameleon_gen.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/curop.h"
#include "mongo/db/read_write_concern_provenance.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/repl/read_concern_level.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/shard_role/transaction_resources.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/str.h"
#include "mongo/util/time_support.h"

namespace mongo::chameleon {

// How far inside the operation deadline the write-concern wait must end (applyWriteConcern).
constexpr int64_t kWriteConcernDeadlineMarginMs = 10;

namespace {

const auto getRequestState = OperationContext::declareDecoration<RequestState>();

int64_t steadyNanos() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

constexpr int kEventualLocal = static_cast<int>(ReadLevel::kEventualLocal);
constexpr int kEventualMajority = static_cast<int>(ReadLevel::kEventualMajority);
constexpr int kCausalLocal = static_cast<int>(ReadLevel::kCausalLocal);
constexpr int kCausalMajority = static_cast<int>(ReadLevel::kCausalMajority);
constexpr int kLinearizable = static_cast<int>(ReadLevel::kLinearizable);

/** The ReadLevel ordinal a read concern expresses. */
int levelFromReadConcern(const repl::ReadConcernArgs& args) {
    const bool causal = static_cast<bool>(args.getArgsAfterClusterTime());
    switch (args.getLevel()) {
        case repl::ReadConcernLevel::kLinearizableReadConcern:
            return kLinearizable;
        case repl::ReadConcernLevel::kMajorityReadConcern:
        case repl::ReadConcernLevel::kSnapshotReadConcern:
            return causal ? kCausalMajority : kEventualMajority;
        default:
            return causal ? kCausalLocal : kEventualLocal;
    }
}

/** Install the read concern a ReadLevel ordinal maps to (level mapping in MONGODB_PORT.md). */
void setReadConcern(OperationContext* opCtx, int level, const boost::optional<Timestamp>& frontier) {
    boost::optional<LogicalTime> afterClusterTime;
    repl::ReadConcernLevel rcLevel = repl::ReadConcernLevel::kLocalReadConcern;
    switch (level) {
        case kEventualLocal:
            break;
        case kEventualMajority:
            rcLevel = repl::ReadConcernLevel::kMajorityReadConcern;
            break;
        case kCausalLocal:
            if (frontier) {
                afterClusterTime = LogicalTime(*frontier);
            }
            break;
        case kCausalMajority:
            rcLevel = repl::ReadConcernLevel::kMajorityReadConcern;
            if (frontier) {
                afterClusterTime = LogicalTime(*frontier);
            }
            break;
        case kLinearizable:
            rcLevel = repl::ReadConcernLevel::kLinearizableReadConcern;
            break;
        default:
            uasserted(ErrorCodes::BadValue, str::stream() << "unknown chameleon read level " << level);
    }
    repl::ReadConcernArgs args(afterClusterTime, rcLevel);
    args.getProvenance().setSource(ReadWriteConcernProvenance::Source::clientSupplied);
    std::lock_guard<Client> lk(*opCtx->getClient());
    repl::ReadConcernArgs::get(opCtx) = std::move(args);
}

double clampBound(double requestedMs) {
    const double maxWait = gChameleonMaxWaitMs.load();
    double bound = requestedMs > 0 ? std::min(std::ceil(requestedMs), maxWait) : maxWait;
    return std::max(1.0, bound);
}

[[noreturn]] void rejectRequest(OperationContext*,
                                RequestState& state,
                                Chameleon* ch,
                                RejectReason reason,
                                const char* message) {
    if (state.chosen && state.chosen->holdsRider) {
        ch->riders().release(state.chosen->cell);
        state.chosen->holdsRider = false;
    }
    state.rejected = true;
    ch->recordRejected(reason);
    uasserted(ErrorCodes::ChameleonRejected, message);
}

void chargeReplication(OperationContext* opCtx, RequestState& state, Chameleon* ch, double entries) {
    state.estimatedEntries = entries;
    if (!ch->replicationBucket().tryCharge(entries)) {
        rejectRequest(opCtx, state, ch, RejectReason::kReplicationBudget, "replication budget exhausted");
    }
    state.replicationCharged = true;
}

ScoringInputs scoringInputs(Chameleon* ch, RequestState& state, const std::vector<Rung>* sla) {
    ScoringInputs in{sla, state.envelope.rttMs, ch->lambda()};
    const int mode = gChameleonExecEstimatorMode.load();
    if (mode > 0) {
        const HistogramSnapshot* exec = ch->execSnapshot(state.shapeKey);
        if (mode == 1) {
            in.execMeanMs = exec->meanMs();
        } else {
            in.execSnapshot = exec;
        }
    }
    return in;
}

void adoptChoice(RequestState& state, const Candidate& chosen) {
    state.chosen = chosen;
    state.waitCell = chosen.cell;
    state.hasWaitCell = true;
    state.gapBucket = chosen.gapBucket;
    state.predictedProfit = chosen.scored.expectedProfit;
    state.boundMs = clampBound(chosen.scored.dMaxMs);
}

void decideRead(OperationContext* opCtx,
                RequestState& state,
                Chameleon* ch,
                repl::ReplicationCoordinator* replCoord,
                const SlaRegistry::Tables& slas) {
    const Envelope& env = state.envelope;
    const auto* sla = SlaRegistry::find(slas.read, env.applicationId, env.slaId);
    uassert(ErrorCodes::BadValue,
            str::stream() << "no read SLA registered for application " << env.applicationId << " sla "
                          << env.slaId,
            sla);

    const auto lastApplied = replCoord->getMyLastAppliedOpTimeAndWallTime();
    const auto commit = replCoord->getLastCommittedOpTimeAndWallTime();
    const GapBuckets buckets = ch->gapBuckets();
    const ScoringInputs in = scoringInputs(ch, state, sla);

    // Step 2: linearizable is legal on the primary only.
    const int maxLevel = state.isPrimary ? kLinearizable : kCausalMajority;
    std::vector<Candidate> candidates;
    std::size_t satisfiableAnywhere = 0;
    for (int level = kEventualLocal; level <= maxLevel; ++level) {
        // Step 3: the gap each level must close; only the causal levels wait on the frontier.
        double gap = 0.0;
        if (env.frontier) {
            if (level == kCausalLocal) {
                gap = ch->gapMs(*env.frontier, lastApplied);
            } else if (level == kCausalMajority) {
                gap = ch->gapMs(*env.frontier, commit);
            }
        }
        const std::size_t bucket = buckets.bucketOf(gap);
        const std::size_t cell = CellIndex::read(static_cast<ReadLevel>(level), bucket);
        candidates.push_back(scoreCandidate(in, level, cell, bucket, *ch->waitHistograms().snapshot(cell)));
        satisfiableAnywhere += candidates.back().scored.satisfiableRungs;
    }
    // An SLA that only pays for linearizable, on a secondary: redirect rather than reject.
    uassert(ErrorCodes::NotWritablePrimary,
            "no rung of this SLA is satisfiable on a secondary",
            satisfiableAnywhere > 0 || state.isPrimary);

    auto chosen = choose(candidates, ch->riders());
    if (!chosen) {
        rejectRequest(opCtx, state, ch, RejectReason::kScorer, "no consistency level is worth its price");
    }
    adoptChoice(state, *chosen);
    setReadConcern(opCtx, chosen->strength, env.frontier);
    if (chosen->strength == kLinearizable) {
        // The confirmation noop is one oplog entry.
        chargeReplication(opCtx, state, ch, 1.0);
    }
}

void adoptClientRead(OperationContext* opCtx,
                     RequestState& state,
                     Chameleon* ch,
                     repl::ReplicationCoordinator* replCoord) {
    const Envelope& env = state.envelope;
    const auto& args = repl::ReadConcernArgs::get(opCtx);
    const int level = levelFromReadConcern(args);
    double gap = 0.0;
    if (env.frontier) {
        if (level == kCausalLocal) {
            gap = ch->gapMs(*env.frontier, replCoord->getMyLastAppliedOpTimeAndWallTime());
        } else if (level == kCausalMajority) {
            gap = ch->gapMs(*env.frontier, replCoord->getLastCommittedOpTimeAndWallTime());
        }
    }
    state.gapBucket = ch->gapBuckets().bucketOf(gap);
    state.waitCell = CellIndex::read(static_cast<ReadLevel>(level), state.gapBucket);
    state.hasWaitCell = true;
    state.boundMs = clampBound(env.waitBoundMs);
    if (level == kLinearizable && state.isPrimary) {
        chargeReplication(opCtx, state, ch, 1.0);
    }
}

void decideWrite(OperationContext* opCtx,
                 RequestState& state,
                 Chameleon* ch,
                 const SlaRegistry::Tables& slas) {
    if (!state.isPrimary) {
        return;  // the command fails with NotWritablePrimary downstream; that is the redirect
    }
    const Envelope& env = state.envelope;
    const auto* sla = SlaRegistry::find(slas.write, env.applicationId, env.slaId);
    uassert(ErrorCodes::BadValue,
            str::stream() << "no write SLA registered for application " << env.applicationId << " sla "
                          << env.slaId,
            sla);
    const ScoringInputs in = scoringInputs(ch, state, sla);
    const int majority = ch->majority();
    std::vector<Candidate> candidates;
    for (int wc = 1; wc <= majority; ++wc) {
        const std::size_t cell = CellIndex::write(wc);
        candidates.push_back(scoreCandidate(in, wc, cell, 0, *ch->waitHistograms().snapshot(cell)));
    }
    auto chosen = choose(candidates, ch->riders());
    if (!chosen) {
        rejectRequest(opCtx, state, ch, RejectReason::kScorer, "no write concern is worth its price");
    }
    adoptChoice(state, *chosen);
    state.deliveredWriteConcern = chosen->strength;
    // One entry per admitted single-document write, trued up at completion.
    chargeReplication(opCtx, state, ch, 1.0);
}

void adoptClientWrite(OperationContext* opCtx, RequestState& state, Chameleon* ch) {
    if (!state.isPrimary) {
        return;
    }
    const Envelope& env = state.envelope;
    const int majority = ch->majority();
    if (env.requestedWriteConcern > 0) {
        state.deliveredWriteConcern = std::clamp(env.requestedWriteConcern, 1, majority);
    }
    state.boundMs = clampBound(env.waitBoundMs);
    state.waitCell = CellIndex::write(std::max(1, state.deliveredWriteConcern));
    state.hasWaitCell = true;
    chargeReplication(opCtx, state, ch, 1.0);
}

void collectShape(const BSONObj& obj, const std::string& prefix, std::vector<std::string>* out) {
    for (const BSONElement& element : obj) {
        std::string name = prefix + std::string{element.fieldNameStringData()};
        out->push_back(name + ":" + std::to_string(static_cast<int>(element.type())));
        if (element.type() == BSONType::object) {
            collectShape(element.Obj(), name + ".", out);
        }
    }
}

std::string shapeString(const BSONObj& body) {
    std::string shape;
    const BSONElement first = body.firstElement();
    const std::string command{first.fieldNameStringData()};
    shape += command;
    if (first.type() == BSONType::string) {
        shape += "|" + std::string{first.valueStringData()};
    }
    std::vector<std::string> keys;
    auto addPredicate = [&](const BSONElement& predicate) {
        if (predicate.type() == BSONType::object) {
            collectShape(predicate.Obj(), "", &keys);
            std::sort(keys.begin(), keys.end());
            for (const std::string& key : keys) {
                shape += "|" + key;
            }
            keys.clear();
        }
    };
    if (command == "find" || command == "count" || command == "distinct") {
        addPredicate(body["filter"]);
        if (body.hasField("query")) {
            addPredicate(body["query"]);
        }
        for (const char* option : {"sort", "projection", "limit", "skip", "hint"}) {
            if (body.hasField(option)) {
                shape += std::string{"|"} + option;
            }
        }
    } else if (command == "update" || command == "delete") {
        const BSONElement statements = body[command == "update" ? "updates" : "deletes"];
        if (statements.type() == BSONType::array) {
            const auto array = statements.Array();
            shape += "|n=" + std::to_string(array.size());
            if (!array.empty() && array.front().type() == BSONType::object) {
                const BSONObj statement = array.front().Obj();
                addPredicate(statement["q"]);
                if (command == "update") {
                    addPredicate(statement["u"]);
                    shape += statement["multi"].trueValue() ? "|multi" : "|single";
                    shape += statement["upsert"].trueValue() ? "|upsert" : "";
                } else {
                    shape += statement["limit"].numberInt() == 1 ? "|single" : "|multi";
                }
            }
        }
    } else if (command == "aggregate") {
        const BSONElement pipeline = body["pipeline"];
        if (pipeline.type() == BSONType::array) {
            for (const BSONElement& stage : pipeline.Array()) {
                if (stage.type() == BSONType::object) {
                    shape += "|" + std::string{stage.Obj().firstElementFieldNameStringData()};
                }
            }
        }
    } else if (command == "insert") {
        const BSONElement documents = body["documents"];
        shape += "|n=" + std::to_string(documents.type() == BSONType::array ? documents.Array().size() : 0);
    }
    return shape;
}

}  // namespace

// ===== Envelope and shape =====

boost::optional<Envelope> parseEnvelope(const boost::optional<BSONElement>& comment) {
    if (!comment || comment->type() != BSONType::object) {
        return boost::none;
    }
    const BSONObj obj = comment->Obj();
    const BSONElement app = obj["app"];
    if (app.eoo()) {
        return boost::none;
    }
    uassert(ErrorCodes::BadValue, "chameleon envelope: app must be a number", app.isNumber());
    const BSONElement sla = obj["sla"];
    uassert(ErrorCodes::BadValue, "chameleon envelope: sla must be a number", sla.isNumber());
    const BSONElement read = obj["read"];
    uassert(ErrorCodes::BadValue, "chameleon envelope: read must be a boolean", read.type() == BSONType::boolean);

    Envelope env;
    env.applicationId = app.numberInt();
    env.slaId = sla.numberInt();
    env.isRead = read.boolean();
    if (const BSONElement rtt = obj["rtt"]; !rtt.eoo()) {
        uassert(ErrorCodes::BadValue, "chameleon envelope: rtt must be a number", rtt.isNumber());
        env.rttMs = std::max(0.0, rtt.numberDouble());
    }
    if (const BSONElement frontier = obj["frontier"]; !frontier.eoo()) {
        uassert(ErrorCodes::BadValue,
                "chameleon envelope: frontier must be a timestamp",
                frontier.type() == BSONType::timestamp);
        env.frontier = frontier.timestamp();
    }
    if (const BSONElement target = obj["target"]; !target.eoo()) {
        uassert(ErrorCodes::BadValue,
                "chameleon envelope: target must be a read level ordinal 0..4",
                target.isNumber() && target.numberInt() >= 0 && target.numberInt() < kReadLevels);
        env.targetLevel = target.numberInt();
    }
    if (const BSONElement wantLin = obj["wantLin"]; !wantLin.eoo()) {
        uassert(ErrorCodes::BadValue,
                "chameleon envelope: wantLin must be a boolean",
                wantLin.type() == BSONType::boolean);
        env.wantLinearizable = wantLin.boolean();
    }
    if (const BSONElement wc = obj["wc"]; !wc.eoo()) {
        uassert(ErrorCodes::BadValue, "chameleon envelope: wc must be a number", wc.isNumber());
        env.requestedWriteConcern = wc.numberInt();
    }
    if (const BSONElement bound = obj["waitBound"]; !bound.eoo()) {
        uassert(ErrorCodes::BadValue, "chameleon envelope: waitBound must be a number", bound.isNumber());
        env.waitBoundMs = bound.numberDouble();
    }
    return env;
}

uint64_t shapeKeyOf(const BSONObj& commandBody) {
    return std::hash<std::string>{}(shapeString(commandBody));
}

RequestState& RequestState::get(OperationContext* opCtx) {
    return getRequestState(opCtx);
}

// ===== Hooks =====

void onInitiateCommand(OperationContext* opCtx, const CommandInvocation*, const BSONObj& commandBody) {
    Chameleon* ch = Chameleon::get(opCtx);
    if (!ch || !gChameleonEnabled.load()) {
        return;
    }
    auto envelope = parseEnvelope(opCtx->getComment());
    if (!envelope) {
        return;
    }
    RequestState& state = RequestState::get(opCtx);
    state.active = true;
    state.envelope = *envelope;
    // The per-shape execution histogram is consumed only by the exec estimator
    // (chameleonExecEstimatorMode 1 or 2); with it off, neither the key nor the
    // sample is computed, in any arm.
    state.shapeKeyed = gChameleonExecEstimatorMode.load() > 0;
    state.shapeKey = state.shapeKeyed ? shapeKeyOf(commandBody) : 0;
    state.serverDecided = gChameleonServerDecision.load();

    auto* replCoord = repl::ReplicationCoordinator::get(opCtx);
    state.isPrimary = replCoord && replCoord->getMemberState().primary();

    // Step 5 hard backstop, in every arm: no free slot against chameleonHardCapInFlight,
    // the Java store's server.ingressHardCapInFlight pushed by the driver (3 x sMax in the
    // shipping configs). It bounds in-flight when the price is silenced or overwhelmed.
    const int hardCap = gChameleonHardCapInFlight.load();
    if (ch->occupancy().inFlight() >= hardCap) {
        rejectRequest(opCtx, state, ch, RejectReason::kOccupancyCap, "occupancy hard cap reached");
    }
    ch->occupancy().onEvent(+1);
    state.occupancyHeld = true;

    auto slas = globalSlaRegistry().snapshot();
    if (state.envelope.isRead) {
        if (state.serverDecided) {
            decideRead(opCtx, state, ch, replCoord, *slas);
        } else {
            adoptClientRead(opCtx, state, ch, replCoord);
        }
    } else {
        if (state.serverDecided) {
            decideWrite(opCtx, state, ch, *slas);
        } else {
            adoptClientWrite(opCtx, state, ch);
        }
    }
    ch->recordAdmitted();
}

void applyWriteConcern(OperationContext* opCtx, WriteConcernOptions* extracted) {
    RequestState& state = RequestState::get(opCtx);
    if (!state.active || state.envelope.isRead || state.deliveredWriteConcern <= 0) {
        return;
    }
    // The acknowledgment wait ends through wtimeout (WriteConcernTimeout, code 64), which
    // the reply carries as an ordinary writeConcernError and the client grades as a
    // fall-back, exactly like the Java store's bounded wait. It must never end through the
    // operation's maxTimeMS: a writeConcernError with an execution-timeout code makes the
    // Java driver mark the server UNKNOWN and stall every request bound for it. So the
    // bound is clamped strictly inside whatever remains of the operation deadline.
    int64_t waitMs = static_cast<int64_t>(state.boundMs);
    if (opCtx->hasDeadline()) {
        const auto remaining = opCtx->getDeadline() - Date_t::now();
        const int64_t remainingMs = durationCount<Milliseconds>(remaining) - kWriteConcernDeadlineMarginMs;
        waitMs = std::min<int64_t>(waitMs, std::max<int64_t>(1, remainingMs));
    }
    // A concern at the write majority goes as w:"majority", not as a count. A set larger
    // than seven members has non-voting members, which acknowledge a numeric w but do not
    // count toward the commit point; "majority" waits for a majority of the voters, which
    // is the commit rule. The reply still reports the count (deliveredWriteConcern).
    Chameleon* ch = Chameleon::get(opCtx);
    const bool majorityWrite = ch && state.deliveredWriteConcern >= ch->majority();
    WriteConcernOptions wc = majorityWrite
        ? WriteConcernOptions(std::string(WriteConcernOptions::kMajority),
                              WriteConcernOptions::SyncMode::UNSET,
                              Milliseconds(waitMs))
        : WriteConcernOptions(state.deliveredWriteConcern,
                              WriteConcernOptions::SyncMode::UNSET,
                              Milliseconds(waitMs));
    wc.getProvenance().setSource(ReadWriteConcernProvenance::Source::clientSupplied);
    *extracted = wc;
}

void waitForReadConcernBounded(OperationContext* opCtx, const std::function<void()>& wait) {
    RequestState& state = RequestState::get(opCtx);
    if (!state.active || state.boundMs <= 0) {
        wait();
        return;
    }
    const auto& args = repl::ReadConcernArgs::get(opCtx);
    const bool mayWait = static_cast<bool>(args.getArgsAfterClusterTime()) ||
        args.getLevel() == repl::ReadConcernLevel::kMajorityReadConcern ||
        args.getLevel() == repl::ReadConcernLevel::kSnapshotReadConcern;
    if (!mayWait) {
        wait();
        return;
    }
    const int64_t start = steadyNanos();
    const Date_t operationDeadline = opCtx->getDeadline();
    const Date_t bound = Date_t::now() + Milliseconds(static_cast<int64_t>(state.boundMs));
    try {
        opCtx->runWithDeadline(bound, ErrorCodes::MaxTimeMSExpired, [&] { wait(); });
    } catch (const ExceptionFor<ErrorCategory::ExceededTimeLimitError>&) {
        if (Date_t::now() >= operationDeadline) {
            throw;  // the operation's own maxTimeMS expired, not our bound
        }
        // Step 6 fallback: the strongest level that needs no waiting, keeping the majority-ness.
        state.fellBack = true;
        const int level = levelFromReadConcern(args);
        const int fallback =
            (level == kCausalMajority || level == kEventualMajority) ? kEventualMajority : kEventualLocal;
        setReadConcern(opCtx, fallback, boost::none);
        wait();
    }
    const int64_t micros = (steadyNanos() - start) / 1000;
    state.waitMicros += micros;
    if (micros > 500) {
        state.waited = true;
    }
}

Milliseconds linearizableWaitTimeout(OperationContext* opCtx) {
    RequestState& state = RequestState::get(opCtx);
    if (!state.active || state.boundMs <= 0) {
        return Milliseconds::zero();
    }
    state.linearizableWaitStartNanos = steadyNanos();
    return Milliseconds(static_cast<int64_t>(state.boundMs));
}

bool absorbLinearizableWaitResult(OperationContext* opCtx, const Status& status) {
    RequestState& state = RequestState::get(opCtx);
    if (!state.active) {
        return false;
    }
    if (state.linearizableWaitStartNanos != 0) {
        state.waitMicros += (steadyNanos() - state.linearizableWaitStartNanos) / 1000;
        state.linearizableWaitStartNanos = 0;
    }
    state.waited = true;
    if (status == ErrorCodes::LinearizableReadConcernError) {
        // The confirmation round did not finish inside the bound: the data already
        // read stands, graded as the strongest no-wait level a primary serves.
        state.fellBack = true;
        state.deliveredLevel = kEventualLocal;
        return true;
    }
    return false;
}

void appendReplyEnvelope(OperationContext* opCtx, BSONObjBuilder* bob) {
    RequestState& state = RequestState::get(opCtx);
    if (!state.active || state.replyAppended) {
        return;
    }
    state.replyAppended = true;
    auto* replCoord = repl::ReplicationCoordinator::get(opCtx);
    const auto lastApplied = replCoord->getMyLastAppliedOpTimeAndWallTime();
    const auto commit = replCoord->getLastCommittedOpTimeAndWallTime();

    BSONObjBuilder env(bob->subobjStart("$chameleon"));
    const int level = state.deliveredLevel >= 0
        ? state.deliveredLevel
        : levelFromReadConcern(repl::ReadConcernArgs::get(opCtx));
    env.append("level", level);
    env.append("wc", state.deliveredWriteConcern);
    env.append("lastApplied", lastApplied.opTime.getTimestamp());
    env.append("commit", commit.opTime.getTimestamp());
    if (state.envelope.isRead) {
        // The snapshot the read used; an untimestamped read on the primary is
        // bounded by lastApplied at reply time.
        Timestamp readTs = lastApplied.opTime.getTimestamp();
        if (auto* ru = shard_role_details::getRecoveryUnit(opCtx)) {
            if (auto used = ru->getLastUsedReadTimestamp()) {
                readTs = *used;
            }
        }
        env.append("readTs", readTs);
    } else {
        env.append("entryTs", repl::ReplClientInfo::forClient(opCtx->getClient()).getLastOp().getTimestamp());
    }
    const double serviceMs =
        static_cast<double>(durationCount<Microseconds>(CurOp::get(opCtx)->elapsedTimeTotal())) / 1000.0;
    env.append("serviceMs", serviceMs);
    env.append("predicted", state.predictedProfit);
    env.append("waited", state.waited);
    env.append("fellBack", state.fellBack);
    env.append("rejected", state.rejected);
    env.append("epochMs", static_cast<long long>(Date_t::now().toMillisSinceEpoch()));
}

void onCompleteOperation(OperationContext* opCtx, CurOp& curOp) {
    RequestState& state = RequestState::get(opCtx);
    if (!state.active) {
        return;
    }
    Chameleon* ch = Chameleon::get(opCtx);
    if (!ch) {
        return;
    }
    if (state.occupancyHeld) {
        ch->occupancy().onEvent(-1);
        state.occupancyHeld = false;
    }
    if (state.chosen && state.chosen->holdsRider) {
        ch->riders().release(state.chosen->cell);
        state.chosen->holdsRider = false;
    }
    if (state.rejected) {
        return;
    }
    const double totalMs =
        static_cast<double>(durationCount<Microseconds>(curOp.elapsedTimeTotal())) / 1000.0;
    const double writeConcernWaitMs =
        static_cast<double>(durationCount<Milliseconds>(curOp.debug().waitForWriteConcernDurationMillis));
    const double waitMs = static_cast<double>(state.waitMicros) / 1000.0 + writeConcernWaitMs;

    // Step 8: the wait sample goes to the executed level's cell, at the bound when the
    // wait was abandoned; the execution sample goes to the shape's cell.
    if (state.hasWaitCell) {
        ch->waitHistograms().file(state.waitCell, state.fellBack ? state.boundMs : waitMs);
    }
    if (state.shapeKeyed) {
        ch->fileExec(state.shapeKey, std::max(0.0, totalMs - waitMs));
    }
    ch->recordCompleted();

    if (state.replicationCharged && !state.envelope.isRead) {
        const auto& metrics = curOp.debug().getAdditiveMetrics();
        const double actual = static_cast<double>(metrics.nModified.value_or(0) + metrics.ninserted.value_or(0) +
                                                  metrics.ndeleted.value_or(0) + metrics.nUpserted.value_or(0));
        ch->replicationBucket().trueUp(state.estimatedEntries, actual);
    }
}

}  // namespace mongo::chameleon
