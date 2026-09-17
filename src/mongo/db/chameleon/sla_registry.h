/**
 * Registered SLAs, keyed by (applicationId, slaId), one table for reads and
 * one for writes. Requests carry only the identity; the decision policy looks
 * the rungs up here. Replaced wholesale when the driver sets the
 * chameleonSlaTable server parameter; lookups take a shared snapshot so a
 * replacement never invalidates rungs a request is scoring.
 *
 * Standard library only; the BSON/JSON parsing lives in the mongod glue.
 */
#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "mongo/db/chameleon/rung_scorer.h"

namespace mongo::chameleon {

class SlaRegistry {
public:
    using Key = std::pair<int32_t, int32_t>;  // (applicationId, slaId)
    using Table = std::map<Key, std::vector<Rung>>;

    struct Tables {
        Table read;
        Table write;
    };

    /** Install a new set of tables; readers holding the previous snapshot keep it. */
    void replace(Tables tables) {
        auto fresh = std::make_shared<const Tables>(std::move(tables));
        std::lock_guard<std::mutex> lock(_mutex);
        _tables = std::move(fresh);
    }

    std::shared_ptr<const Tables> snapshot() const {
        std::lock_guard<std::mutex> lock(_mutex);
        return _tables;
    }

    /** nullptr when the SLA is not registered. */
    static const std::vector<Rung>* find(const Table& table, int32_t applicationId, int32_t slaId) {
        auto it = table.find(Key{applicationId, slaId});
        return it == table.end() ? nullptr : &it->second;
    }

private:
    mutable std::mutex _mutex;
    std::shared_ptr<const Tables> _tables = std::make_shared<const Tables>();
};

}  // namespace mongo::chameleon
