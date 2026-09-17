#include "mongo/db/chameleon/chameleon.h"

#include <algorithm>
#include <chrono>
#include <cmath>

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "mongo/db/chameleon/chameleon_gen.h"
#include "mongo/db/client.h"
#include "mongo/db/commands/server_status/server_status.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/repl_set_config.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/server_parameter.h"
#include "mongo/db/server_parameter_with_storage.h"
#include "mongo/logv2/log.h"
#include "mongo/util/str.h"
#include "mongo/util/time_support.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace mongo::chameleon {

namespace {

const auto getChameleon = ServiceContext::declareDecoration<std::unique_ptr<Chameleon>>();

int64_t steadyNanos() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

class ChameleonServerStatusSection : public ServerStatusSection {
public:
    using ServerStatusSection::ServerStatusSection;

    bool includeByDefault() const override {
        return true;
    }

    BSONObj generateSection(OperationContext* opCtx, const BSONElement&) const override {
        auto* instance = Chameleon::get(opCtx);
        if (!instance) {
            return {};
        }
        return instance->generateSection();
    }
};

auto& chameleonSection =
    *ServerStatusSectionBuilder<ChameleonServerStatusSection>("chameleon").forShard();

}  // namespace

// ===== SLA registry and its parameter =====

SlaRegistry& globalSlaRegistry() {
    static SlaRegistry* registry = new SlaRegistry();
    return *registry;
}

namespace {

Status parseTable(const BSONElement& tableElement, const char* name, SlaRegistry::Table* out) {
    if (tableElement.eoo()) {
        return Status::OK();
    }
    if (tableElement.type() != BSONType::array) {
        return {ErrorCodes::TypeMismatch, str::stream() << "chameleonSlaTable." << name << " must be an array"};
    }
    for (const BSONElement& slaElement : tableElement.Array()) {
        if (slaElement.type() != BSONType::object) {
            return {ErrorCodes::TypeMismatch,
                    str::stream() << "chameleonSlaTable." << name << " entries must be objects"};
        }
        BSONObj sla = slaElement.Obj();
        BSONElement app = sla["app"];
        BSONElement slaId = sla["sla"];
        BSONElement rungs = sla["rungs"];
        if (!app.isNumber() || !slaId.isNumber()) {
            return {ErrorCodes::BadValue,
                    str::stream() << "chameleonSlaTable." << name << " entry needs numeric app and sla"};
        }
        if (rungs.type() != BSONType::array || rungs.Array().empty()) {
            return {ErrorCodes::BadValue,
                    str::stream() << "chameleonSlaTable." << name << " entry (app " << app.numberInt()
                                  << ", sla " << slaId.numberInt() << ") needs a non-empty rungs array"};
        }
        std::vector<Rung> parsed;
        for (const BSONElement& rungElement : rungs.Array()) {
            if (rungElement.type() != BSONType::object) {
                return {ErrorCodes::TypeMismatch, "chameleonSlaTable rungs must be objects"};
            }
            BSONObj rung = rungElement.Obj();
            BSONElement strength = rung["strength"];
            BSONElement latency = rung["latencyMs"];
            BSONElement profit = rung["profit"];
            if (!strength.isNumber() || !latency.isNumber() || !profit.isNumber()) {
                return {ErrorCodes::BadValue,
                        "chameleonSlaTable rungs need numeric strength, latencyMs and profit"};
            }
            if (latency.numberDouble() <= 0 || profit.numberDouble() <= 0) {
                return {ErrorCodes::BadValue, "chameleonSlaTable rung latencyMs and profit must be positive"};
            }
            parsed.push_back(Rung{strength.numberInt(), latency.numberDouble(), profit.numberDouble()});
        }
        SlaRegistry::Key key{app.numberInt(), slaId.numberInt()};
        if (out->count(key)) {
            return {ErrorCodes::BadValue,
                    str::stream() << "chameleonSlaTable." << name << " registers (app " << key.first
                                  << ", sla " << key.second << ") twice"};
        }
        (*out)[key] = std::move(parsed);
    }
    return Status::OK();
}

void appendTable(BSONObjBuilder* bob, const char* name, const SlaRegistry::Table& table) {
    BSONArrayBuilder entries(bob->subarrayStart(name));
    for (const auto& [key, rungs] : table) {
        BSONObjBuilder entry(entries.subobjStart());
        entry.append("app", key.first);
        entry.append("sla", key.second);
        BSONArrayBuilder rungArray(entry.subarrayStart("rungs"));
        for (const Rung& rung : rungs) {
            rungArray.append(BSON("strength" << rung.strength << "latencyMs" << rung.thresholdMs
                                             << "profit" << rung.profit));
        }
    }
}

}  // namespace

Status parseSlaTables(const BSONObj& doc, SlaRegistry::Tables* out) {
    SlaRegistry::Tables tables;
    if (auto status = parseTable(doc["read"], "read", &tables.read); !status.isOK()) {
        return status;
    }
    if (auto status = parseTable(doc["write"], "write", &tables.write); !status.isOK()) {
        return status;
    }
    *out = std::move(tables);
    return Status::OK();
}

BSONObj serializeSlaTables(const SlaRegistry::Tables& tables) {
    BSONObjBuilder bob;
    appendTable(&bob, "read", tables.read);
    appendTable(&bob, "write", tables.write);
    return bob.obj();
}

void ChameleonSlaTableParameter::append(OperationContext*,
                                        BSONObjBuilder* b,
                                        std::string_view name,
                                        const boost::optional<TenantId>&) {
    b->append(name, serializeSlaTables(*globalSlaRegistry().snapshot()));
}

Status ChameleonSlaTableParameter::set(const BSONElement& newValueElement,
                                       const boost::optional<TenantId>&) {
    if (newValueElement.type() != BSONType::object) {
        return {ErrorCodes::TypeMismatch, "chameleonSlaTable must be a document {read: [...], write: [...]}"};
    }
    SlaRegistry::Tables tables;
    if (auto status = parseSlaTables(newValueElement.Obj(), &tables); !status.isOK()) {
        return status;
    }
    LOGV2(99100001,
          "Chameleon SLA table installed",
          "readSlas"_attr = tables.read.size(),
          "writeSlas"_attr = tables.write.size());
    globalSlaRegistry().replace(std::move(tables));
    return Status::OK();
}

Status ChameleonSlaTableParameter::setFromString(std::string_view value,
                                                 const boost::optional<TenantId>& tenant) {
    try {
        BSONObj wrapped = BSON("v" << fromjson(std::string{value}));
        return set(wrapped.firstElement(), tenant);
    } catch (const DBException& e) {
        return e.toStatus().withContext("chameleonSlaTable is not valid JSON");
    }
}

// ===== Chameleon =====

Chameleon* Chameleon::get(ServiceContext* service) {
    return getChameleon(service).get();
}

Chameleon* Chameleon::get(OperationContext* opCtx) {
    return get(opCtx->getServiceContext());
}

void Chameleon::set(ServiceContext* service, std::unique_ptr<Chameleon> instance) {
    getChameleon(service) = std::move(instance);
}

void Chameleon::shutdown(ServiceContext* service) {
    auto& instance = getChameleon(service);
    if (instance) {
        instance->_jobAnchor.stop();
        instance.reset();
    }
}

Chameleon::Chameleon(ServiceContext* service)
    : _service(service),
      _waitHistograms(CellIndex::numCells(kMaxWriteConcern), gChameleonHistogramDecay.load()),
      _riders(CellIndex::numCells(kMaxWriteConcern)),
      _price(gChameleonUTarget.load(), gChameleonEta.load(), gChameleonLambdaMin.load()),
      _replicationBucket(gChameleonReplicationBudgetPerSecond.load()) {
    _configuredBudget.store(gChameleonReplicationBudgetPerSecond.load());
    _jobAnchor = service->getPeriodicRunner()->makeJob(
        {"ChameleonController",
         [this](Client*) { tick(); },
         Milliseconds(gChameleonControlIntervalMs.load()),
         false /*isKillableByStepdown*/});
    _jobAnchor.start();

    using ParamT = IDLServerParameterWithStorage<ServerParameterType::kStartupAndRuntime, Atomic<int>>;
    ServerParameterSet::getNodeParameterSet()
        ->get<ParamT>("chameleonControlIntervalMs")
        ->setOnUpdate([this](const int newValue) -> Status {
            _jobAnchor.setPeriod(Milliseconds(newValue));
            return Status::OK();
        });
}

Chameleon::~Chameleon() = default;

GapBuckets Chameleon::gapBuckets() const {
    return GapBuckets(gChameleonGapEdge1Ms.load(), gChameleonGapEdge2Ms.load());
}

double Chameleon::gapMs(Timestamp frontier, const repl::OpTimeAndWallTime& target) const {
    if (frontier <= target.opTime.getTimestamp()) {
        return 0.0;
    }
    double rate = std::max(1.0, _entriesPerSecond.load(std::memory_order_relaxed));
    double fractionMs = std::min(999.0, static_cast<double>(frontier.getInc()) / rate * 1000.0);
    double frontierMs = static_cast<double>(frontier.getSecs()) * 1000.0 + fractionMs;
    double targetMs = static_cast<double>(target.wallTime.toMillisSinceEpoch());
    // The frontier is ahead of the view, so the gap is positive even when the
    // wall clocks of the two nodes disagree; 1 ms is the smallest positive gap.
    return std::max(1.0, frontierMs - targetMs);
}

const HistogramSnapshot* Chameleon::execSnapshot(uint64_t shapeKey) {
    {
        std::shared_lock<std::shared_mutex> lock(_exec.mutex);
        auto it = _exec.cells.find(shapeKey);
        if (it != _exec.cells.end()) {
            return it->second->snapshot();
        }
        if (_exec.cells.size() >= kMaxExecShapes) {
            return _exec.overflow.snapshot();
        }
    }
    std::unique_lock<std::shared_mutex> lock(_exec.mutex);
    auto existing = _exec.cells.find(shapeKey);
    if (existing != _exec.cells.end()) {
        return existing->second->snapshot();
    }
    if (_exec.cells.size() >= kMaxExecShapes) {
        return _exec.overflow.snapshot();
    }
    auto [it, inserted] = _exec.cells.emplace(shapeKey, std::make_unique<HistogramCell>());
    return it->second->snapshot();
}

void Chameleon::fileExec(uint64_t shapeKey, double execMs) {
    static thread_local const std::size_t stripe =
        std::hash<const void*>{}(static_cast<const void*>(&stripe)) % HistogramCell::kStripes;
    std::shared_lock<std::shared_mutex> lock(_exec.mutex);
    auto it = _exec.cells.find(shapeKey);
    if (it != _exec.cells.end()) {
        it->second->file(execMs, stripe);
    } else {
        _exec.overflow.file(execMs, stripe);
    }
}

std::size_t Chameleon::execShapeCount() const {
    std::shared_lock<std::shared_mutex> lock(_exec.mutex);
    return _exec.cells.size();
}

void Chameleon::recordRejected(RejectReason reason) {
    switch (reason) {
        case RejectReason::kScorer:
            _rejectedScorer.fetch_add(1, std::memory_order_relaxed);
            break;
        case RejectReason::kReplicationBudget:
            _rejectedReplication.fetch_add(1, std::memory_order_relaxed);
            break;
        case RejectReason::kOccupancyCap:
            _rejectedOccupancy.fetch_add(1, std::memory_order_relaxed);
            break;
    }
}

void Chameleon::refreshMajorityAndRate() {
    auto* replCoord = repl::ReplicationCoordinator::get(_service);
    if (!replCoord || !replCoord->getSettings().isReplSet()) {
        _majority.store(1, std::memory_order_relaxed);
        return;
    }
    int majority = replCoord->getConfig().getWriteMajority();
    _majority.store(std::clamp(majority, 1, kMaxWriteConcern), std::memory_order_relaxed);

    // Entry rate: the increment of the hybrid logical clock counts oplog
    // entries within one wall second, so within a second the increment
    // difference over the tick interval is a rate sample.
    Timestamp lastApplied = replCoord->getMyLastAppliedOpTime().getTimestamp();
    int64_t now = steadyNanos();
    if (_lastAppliedSampleNanos != 0 && lastApplied.getSecs() == _lastAppliedSample.getSecs() &&
        lastApplied.getInc() >= _lastAppliedSample.getInc() && now > _lastAppliedSampleNanos) {
        double seconds = static_cast<double>(now - _lastAppliedSampleNanos) / 1e9;
        double sample = static_cast<double>(lastApplied.getInc() - _lastAppliedSample.getInc()) / seconds;
        double previous = _entriesPerSecond.load(std::memory_order_relaxed);
        _entriesPerSecond.store(0.8 * previous + 0.2 * sample, std::memory_order_relaxed);
    }
    _lastAppliedSample = lastApplied;
    _lastAppliedSampleNanos = now;
}

void Chameleon::tick() {
    // Histograms: fold pending samples with the configured decay and publish.
    double decay = gChameleonHistogramDecay.load();
    if (decay != _waitHistograms.decay()) {
        _waitHistograms.setDecay(decay);
    }
    _waitHistograms.refreshTick();
    {
        std::shared_lock<std::shared_mutex> lock(_exec.mutex);
        for (auto& [key, cell] : _exec.cells) {
            cell->refreshTick(decay);
        }
        _exec.overflow.refreshTick(decay);
    }

    // Price controller: u = slot-time / (S_max * interval).
    OccupancyMeter::Interval interval = _occupancy.closeInterval();
    if (interval.intervalNanos > 0) {
        double intervalMs = static_cast<double>(interval.intervalNanos) / 1e6;
        double slotMs = static_cast<double>(interval.slotNanos) / 1e6;
        double utilization = slotMs / (gChameleonSMax.load() * intervalMs);
        _price.reconfigure(gChameleonUTarget.load(), gChameleonEta.load(), gChameleonLambdaMin.load());
        _price.update(utilization);
        _lastUtilization.store(utilization, std::memory_order_relaxed);
        _lastAverageInFlight.store(interval.averageInFlight(), std::memory_order_relaxed);
        _lastInFlightAtClose.store(interval.inFlightAtClose, std::memory_order_relaxed);
    }

    double budget = gChameleonReplicationBudgetPerSecond.load();
    if (budget != _configuredBudget.load(std::memory_order_relaxed)) {
        _replicationBucket.reconfigure(budget);
        _configuredBudget.store(budget, std::memory_order_relaxed);
    }

    refreshMajorityAndRate();
}

BSONObj Chameleon::generateSection() const {
    BSONObjBuilder bob;
    bob.append("enabled", gChameleonEnabled.load());
    bob.append("serverDecision", gChameleonServerDecision.load());
    bob.append("u", _lastUtilization.load(std::memory_order_relaxed));
    bob.append("avgInFlight", _lastAverageInFlight.load(std::memory_order_relaxed));
    bob.append("inFlightAtClose", _lastInFlightAtClose.load(std::memory_order_relaxed));
    bob.append("inFlight", _occupancy.inFlight());
    bob.append("lambda", _price.lambda());
    bob.append("sMax", gChameleonSMax.load());
    bob.append("majority", _majority.load(std::memory_order_relaxed));
    bob.append("entriesPerSecond", _entriesPerSecond.load(std::memory_order_relaxed));
    bob.append("replicationTokens", const_cast<ReplicationRateBucket&>(_replicationBucket).tokensRemaining());
    bob.append("admitted", static_cast<long long>(_admitted.load(std::memory_order_relaxed)));
    bob.append("completed", static_cast<long long>(_completed.load(std::memory_order_relaxed)));
    {
        BSONObjBuilder rejected(bob.subobjStart("rejected"));
        rejected.append("scorer", static_cast<long long>(_rejectedScorer.load(std::memory_order_relaxed)));
        rejected.append("replicationBudget",
                        static_cast<long long>(_rejectedReplication.load(std::memory_order_relaxed)));
        rejected.append("occupancyCap",
                        static_cast<long long>(_rejectedOccupancy.load(std::memory_order_relaxed)));
    }
    bob.append("execShapes", static_cast<long long>(execShapeCount()));
    auto slas = globalSlaRegistry().snapshot();
    bob.append("readSlas", static_cast<long long>(slas->read.size()));
    bob.append("writeSlas", static_cast<long long>(slas->write.size()));
    if (auto* replCoord = repl::ReplicationCoordinator::get(_service);
        replCoord && replCoord->getSettings().isReplSet()) {
        bob.append("role", replCoord->getMemberState().toString());
    }
    return bob.obj();
}

}  // namespace mongo::chameleon
