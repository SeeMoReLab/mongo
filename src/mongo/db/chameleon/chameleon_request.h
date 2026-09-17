/**
 * Per-request Chameleon logic (MONGODB_PORT.md, Milestone 2): the envelope a
 * request carries in its comment, the level decision and its application to
 * the operation's read and write concern, the bounded waits with fallback,
 * the $chameleon reply envelope, and the completion accounting. Each function
 * is a hook called from one place in the service entry point; all state lives
 * in an OperationContext decoration so the entry point stays thin.
 */
#pragma once

#include <cstdint>
#include <functional>
#include <optional>

#include <boost/optional.hpp>

#include "mongo/base/status.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/chameleon/level_chooser.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/duration.h"

namespace mongo {
class CommandInvocation;
class CurOp;
struct WriteConcernOptions;
}  // namespace mongo

namespace mongo::chameleon {

/** The request envelope, from the comment argument. */
struct Envelope {
    int32_t applicationId = 0;
    int32_t slaId = 0;
    double rttMs = 0.0;
    bool isRead = true;
    boost::optional<Timestamp> frontier;   // the session's causal frontier; none = nothing observed
    boost::optional<int> targetLevel;      // client-decided target ReadLevel ordinal
    bool wantLinearizable = false;
    int requestedWriteConcern = 0;         // client-decided; 0 = none
    double waitBoundMs = 0.0;              // client-decided wait clamp; <= 0 = chameleonMaxWaitMs
};

/**
 * Parse the envelope out of a comment element. Returns none when the comment
 * is absent or is not a Chameleon envelope (no "app" field); throws on an
 * envelope with malformed fields.
 */
boost::optional<Envelope> parseEnvelope(const boost::optional<BSONElement>& comment);

/** Milestone 5: the admission-time query shape key of a command body. */
uint64_t shapeKeyOf(const BSONObj& commandBody);

/** Everything the hooks accumulate for one request. */
struct RequestState {
    static RequestState& get(OperationContext* opCtx);

    bool active = false;             // envelope present and chameleon enabled
    Envelope envelope;
    bool serverDecided = false;
    bool isPrimary = false;

    std::optional<Candidate> chosen;  // server decision, if any
    std::size_t waitCell = 0;         // wait-histogram cell the executed level runs under
    bool hasWaitCell = false;
    std::size_t gapBucket = 0;
    double predictedProfit = 0.0;
    double boundMs = 0.0;             // clamp on this request's waits

    int deliveredLevel = -1;          // ReadLevel ordinal actually delivered (reads)
    int deliveredWriteConcern = 0;    // acks waited for (writes)
    bool waited = false;
    bool fellBack = false;
    bool rejected = false;

    double estimatedEntries = 0.0;    // replication charge made at admission
    bool replicationCharged = false;
    bool occupancyHeld = false;
    uint64_t shapeKey = 0;
    bool shapeKeyed = false;          // shape key computed (exec estimator on); file the exec sample
    int64_t waitMicros = 0;           // read concern + linearizable confirmation waits
    int64_t linearizableWaitStartNanos = 0;
    bool replyAppended = false;
};

/**
 * Hook 1, in ExecCommandDatabase::_initiateCommand after the read concern is
 * set on the operation: parse the envelope, hold an occupancy slot, and when
 * the server decides, score the legal levels, choose, and apply the choice to
 * the operation's read concern (writes are applied in hook 2). Throws
 * ChameleonRejected when admission sheds the request, and NotWritablePrimary
 * when no rung is satisfiable on a secondary (the redirect).
 */
void onInitiateCommand(OperationContext* opCtx,
                       const CommandInvocation* invocation,
                       const BSONObj& commandBody);

/**
 * Hook 2, in RunCommandAndWaitForWriteConcern::_setup on the extracted write
 * concern, before the entry point installs it on the operation: replace it
 * with the chosen (or client-decided) w and the wait bound as wtimeout. The
 * entry point later asserts that the operation's write concern still equals
 * the extracted one, so the extracted object is what must change.
 */
void applyWriteConcern(OperationContext* opCtx, WriteConcernOptions* extracted);

/**
 * Hook 3, wrapping the read concern wait in the entry point helpers: run
 * 'wait' under the request's bound; on expiry fall back to the strongest
 * level that needs no waiting and run it again. Waits without a bound run
 * unchanged.
 */
void waitForReadConcernBounded(OperationContext* opCtx, const std::function<void()>& wait);

/**
 * Hook 4, in RunCommandImpl::_epilogue: the timeout to pass to
 * waitForLinearizableReadConcern (zero = unbounded, stock behavior), and how
 * to treat its result. Returns true when the status was a bounded-wait expiry
 * that has been absorbed as a fallback (the read result stands, graded as
 * eventual-local); the caller then must not uassert on it.
 */
Milliseconds linearizableWaitTimeout(OperationContext* opCtx);
bool absorbLinearizableWaitResult(OperationContext* opCtx, const Status& status);

/**
 * Hook 5, beside appendClusterAndOperationTime on success and error replies:
 * append the $chameleon envelope the client grades from.
 */
void appendReplyEnvelope(OperationContext* opCtx, BSONObjBuilder* bob);

/**
 * Hook 6, in HandleRequest::completeOperation after completeAndLogOperation:
 * release the occupancy slot and any rider, file the wait and exec samples,
 * true up the replication charge.
 */
void onCompleteOperation(OperationContext* opCtx, CurOp& curOp);

}  // namespace mongo::chameleon
