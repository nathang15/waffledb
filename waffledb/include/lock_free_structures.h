// waffledb/include/lock_free_structures.h
#ifndef LOCK_FREE_STRUCTURES_H
#define LOCK_FREE_STRUCTURES_H

#include <atomic>
#include <memory>
#include <vector>
#include <thread>
#include <array>
#include <functional>
#include <stdexcept>
#include <algorithm>

namespace waffledb
{

    // Lock-free queue implementation using Michael & Scott algorithm
    template <typename T>
    class LockFreeQueue
    {
    private:
        struct Node
        {
            std::atomic<T *> data;
            std::atomic<Node *> next;

            Node() : data(nullptr), next(nullptr) {}
        };

        std::atomic<Node *> head_;
        std::atomic<Node *> tail_;

    public:
        LockFreeQueue()
        {
            Node *dummy = new Node;
            head_.store(dummy);
            tail_.store(dummy);
        }

        ~LockFreeQueue()
        {
            while (Node *oldHead = head_.load())
            {
                head_.store(oldHead->next);
                delete oldHead;
            }
        }

        void enqueue(const T &item)
        {
            Node *newNode = new Node;
            T *data = new T(std::move(item));
            newNode->data.store(data);

            while (true)
            {
                Node *last = tail_.load();
                Node *next = last->next.load();

                if (last == tail_.load())
                {
                    if (next == nullptr)
                    {
                        if (last->next.compare_exchange_weak(next, newNode))
                        {
                            tail_.compare_exchange_weak(last, newNode);
                            break;
                        }
                    }
                    else
                    {
                        tail_.compare_exchange_weak(last, next);
                    }
                }
            }
        }

        bool dequeue(T &result)
        {
            while (true)
            {
                Node *first = head_.load();
                Node *last = tail_.load();
                Node *next = first->next.load();

                if (first == head_.load())
                {
                    if (first == last)
                    {
                        if (next == nullptr)
                        {
                            return false;
                        }
                        tail_.compare_exchange_weak(last, next);
                    }
                    else
                    {
                        T *data = next->data.load();
                        if (data == nullptr)
                        {
                            continue;
                        }

                        if (head_.compare_exchange_weak(first, next))
                        {
                            result = *data;
                            delete data;
                            delete first;
                            return true;
                        }
                    }
                }
            }
        }

        bool empty() const
        {
            Node *first = head_.load();
            Node *last = tail_.load();
            return (first == last) && (first->next.load() == nullptr);
        }
    };

    // Epoch-based memory reclamation
    class EpochManager
    {
    private:
        static constexpr size_t MAX_THREADS = 128;
        static constexpr size_t EPOCH_FREQUENCY = 100;

        struct ThreadEpoch
        {
            std::atomic<uint64_t> epoch{0};
            std::atomic<bool> active{false};
            char padding[64 - sizeof(std::atomic<uint64_t>) - sizeof(std::atomic<bool>)];
        };

        std::atomic<uint64_t> globalEpoch_{0};
        std::array<ThreadEpoch, MAX_THREADS> threadEpochs_;
        thread_local static size_t threadId_;

        struct RetiredPtr
        {
            void *ptr;
            uint64_t epoch;
            std::function<void(void *)> deleter;
        };

        thread_local static std::vector<RetiredPtr> retiredList_;

    public:
        class EpochGuard
        {
        private:
            EpochManager *manager_;
            size_t threadId_;

        public:
            EpochGuard(EpochManager *manager, size_t threadId)
                : manager_(manager), threadId_(threadId)
            {
                manager_->enterEpoch(threadId_);
            }

            ~EpochGuard()
            {
                manager_->exitEpoch(threadId_);
            }

            // Prevent copying
            EpochGuard(const EpochGuard &) = delete;
            EpochGuard &operator=(const EpochGuard &) = delete;
        };

        EpochManager();
        ~EpochManager();

        EpochGuard enter();

        template <typename T>
        void retire(T *ptr)
        {
            retiredList_.push_back({ptr,
                                    globalEpoch_.load(),
                                    [](void *p)
                                    { delete static_cast<T *>(p); }});

            if (retiredList_.size() > EPOCH_FREQUENCY)
            {
                collect();
            }
        }

    private:
        void enterEpoch(size_t threadId);
        void exitEpoch(size_t threadId);
        void collect();
        uint64_t getMinEpoch();
        void advanceEpoch();
    };

    // Wait-free reader for columnar data
    template <typename T>
    class WaitFreeReader
    {
    private:
        struct VersionedData
        {
            std::atomic<uint64_t> version{0};
            std::shared_ptr<T> data;
        };

        static constexpr size_t NUM_VERSIONS = 2;
        std::array<VersionedData, NUM_VERSIONS> versions_;
        std::atomic<size_t> currentIndex_{0};

    public:
        WaitFreeReader() = default;

        void update(std::shared_ptr<T> newData)
        {
            size_t nextIndex = (currentIndex_.load() + 1) % NUM_VERSIONS;
            versions_[nextIndex].data = std::move(newData);
            versions_[nextIndex].version.fetch_add(1);
            currentIndex_.store(nextIndex);
        }

        std::shared_ptr<T> read() const
        {
            while (true)
            {
                size_t index = currentIndex_.load();
                uint64_t version1 = versions_[index].version.load();
                std::shared_ptr<T> data = versions_[index].data;
                uint64_t version2 = versions_[index].version.load();

                if (version1 == version2 && (version1 & 1) == 0)
                {
                    return data;
                }
            }
        }
    };

} // namespace waffledb

#endif // LOCK_FREE_STRUCTURES_H