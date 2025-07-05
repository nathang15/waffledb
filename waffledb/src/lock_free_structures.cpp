// waffledb/src/lock_free_structures.cpp
#include "lock_free_structures.h"
#include <stdexcept>
#include <functional>
#include <algorithm>

namespace waffledb
{

    // Thread-local storage definitions
    thread_local size_t EpochManager::threadId_ = SIZE_MAX;
    thread_local std::vector<EpochManager::RetiredPtr> EpochManager::retiredList_;

    EpochManager::EpochManager()
    {
        // Initialize all thread epochs to 0
        for (auto &epoch : threadEpochs_)
        {
            epoch.epoch.store(0);
            epoch.active.store(false);
        }
    }

    EpochManager::~EpochManager()
    {
        // Clean up any remaining retired pointers
        for (auto &epoch : threadEpochs_)
        {
            if (epoch.active.load())
            {
                // Force cleanup
                collect();
            }
        }
    }

    EpochManager::EpochGuard EpochManager::enter()
    {
        // Assign thread ID if not already assigned
        if (threadId_ == SIZE_MAX)
        {
            // Find an unused slot
            for (size_t i = 0; i < MAX_THREADS; ++i)
            {
                bool expected = false;
                if (threadEpochs_[i].active.compare_exchange_strong(expected, true))
                {
                    threadId_ = i;
                    break;
                }
            }

            if (threadId_ == SIZE_MAX)
            {
                throw std::runtime_error("Too many threads");
            }
        }

        return EpochGuard(this, threadId_);
    }

    void EpochManager::enterEpoch(size_t threadId)
    {
        threadEpochs_[threadId].epoch.store(globalEpoch_.load());
    }

    void EpochManager::exitEpoch(size_t threadId)
    {
        threadEpochs_[threadId].epoch.store(UINT64_MAX);

        // Periodically advance global epoch
        static thread_local size_t exitCount = 0;
        if (++exitCount % 100 == 0)
        {
            advanceEpoch();
        }
    }

    void EpochManager::collect()
    {
        uint64_t minEpoch = getMinEpoch();

        // Remove pointers that can be safely deleted
        auto newEnd = std::remove_if(retiredList_.begin(), retiredList_.end(),
                                     [minEpoch](const RetiredPtr &ptr)
                                     {
                                         if (ptr.epoch < minEpoch)
                                         {
                                             ptr.deleter(ptr.ptr);
                                             return true;
                                         }
                                         return false;
                                     });

        retiredList_.erase(newEnd, retiredList_.end());
    }

    uint64_t EpochManager::getMinEpoch()
    {
        uint64_t minEpoch = globalEpoch_.load();

        for (const auto &threadEpoch : threadEpochs_)
        {
            if (threadEpoch.active.load())
            {
                uint64_t epoch = threadEpoch.epoch.load();
                if (epoch != UINT64_MAX)
                {
                    minEpoch = std::min(minEpoch, epoch);
                }
            }
        }

        return minEpoch;
    }

    void EpochManager::advanceEpoch()
    {
        globalEpoch_.fetch_add(1);
    }

} // namespace waffledb