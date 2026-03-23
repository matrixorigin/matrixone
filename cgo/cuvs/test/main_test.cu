/* 
 * Copyright 2021 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "cuvs_worker.hpp"
#include "test_framework.hpp"
#include <cstdio>
#include <cstdlib>

using namespace matrixone;

thread_local bool current_test_failed = false;

// --- thread_safe_queue_t Tests ---

TEST(ThreadSafeQueueTest, BasicPushPop) {
    thread_safe_queue_t<int> q;
    q.push(1);
    q.push(2);

    int val;
    ASSERT_TRUE(q.pop(val));
    ASSERT_EQ(val, 1);
    ASSERT_TRUE(q.pop(val));
    ASSERT_EQ(val, 2);
}

TEST(ThreadSafeQueueTest, PopEmptyBlocking) {
    thread_safe_queue_t<int> q;
    int val = 0;

    auto fut = std::async(std::launch::async, [&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        q.push(42);
    });

    ASSERT_TRUE(q.pop(val));
    ASSERT_EQ(val, 42);
}

TEST(ThreadSafeQueueTest, StopQueue) {
    thread_safe_queue_t<int> q;
    int val;

    auto fut = std::async(std::launch::async, [&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        q.stop();
    });

    ASSERT_FALSE(q.pop(val)); // Should return false after stop
    ASSERT_TRUE(q.is_stopped());
}

TEST(ThreadSafeQueueTest, PushBlocking) {
    thread_safe_queue_t<int> q;
    q.set_capacity(2);
    
    q.push(1);
    q.push(2);
    
    std::atomic<bool> pushed_third{false};
    std::thread t([&]() {
        q.push(3); // Should block
        pushed_third.store(true);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_FALSE(pushed_third.load());

    int val;
    ASSERT_TRUE(q.pop(val));
    ASSERT_EQ(val, 1);

    // Now the third push should unblock
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_TRUE(pushed_third.load());

    ASSERT_TRUE(q.pop(val));
    ASSERT_EQ(val, 2);
    ASSERT_TRUE(q.pop(val));
    ASSERT_EQ(val, 3);
    
    t.join();
}

TEST(ThreadSafeQueueTest, ProducerConsumerStress) {
    thread_safe_queue_t<int> q;
    q.set_capacity(10);
    const int num_producers = 4;
    const int num_consumers = 4;
    const int items_per_producer = 1000;
    
    std::atomic<int> sum_pushed{0};
    std::atomic<int> sum_popped{0};
    std::atomic<int> count_popped{0};

    auto producer = [&]() {
        for (int i = 0; i < items_per_producer; ++i) {
            q.push(1);
            sum_pushed.fetch_add(1);
        }
    };

    auto consumer = [&]() {
        int val;
        while (q.pop(val)) {
            sum_popped.fetch_add(val);
            count_popped.fetch_add(1);
            if (count_popped.load() == num_producers * items_per_producer) {
                q.stop();
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_producers; ++i) threads.emplace_back(producer);
    for (int i = 0; i < num_consumers; ++i) threads.emplace_back(consumer);

    for (auto& t : threads) t.join();

    ASSERT_EQ(sum_pushed.load(), sum_popped.load());
    ASSERT_EQ(count_popped.load(), num_producers * items_per_producer);
}

TEST(ThreadSafeQueueTest, StopUnblocksProducer) {
    thread_safe_queue_t<int> q;
    q.set_capacity(1);
    q.push(1);

    std::atomic<bool> push_exited{false};
    std::thread t([&]() {
        q.push(2); // Blocks
        push_exited.store(true);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_FALSE(push_exited.load());

    q.stop();
    t.join();
    ASSERT_TRUE(push_exited.load());
}

// --- cuvs_task_result_store_t Tests ---

TEST(CuvsTaskResultStoreTest, BasicStoreRetrieve) {
    cuvs_task_result_store_t store;
    uint64_t id = store.get_next_job_id();
    
    cuvs_task_result_t res{std::any(100), nullptr};
    store.store(id, res);

    auto fut = store.wait(id);
    auto retrieved = fut.get();
    ASSERT_EQ(std::any_cast<int>(retrieved.result), 100);
}

TEST(CuvsTaskResultStoreTest, AsyncWait) {
    cuvs_task_result_store_t store;
    uint64_t id = store.get_next_job_id();

    auto fut = store.wait(id);
    
    std::thread t([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        store.store(id, {std::any(std::string("async")), nullptr});
    });

    auto retrieved = fut.get();
    ASSERT_EQ(std::any_cast<std::string>(retrieved.result), std::string("async"));
    t.join();
}

TEST(CuvsTaskResultStoreTest, StopStore) {
    cuvs_task_result_store_t store;
    uint64_t id = store.get_next_job_id();
    auto fut = store.wait(id);

    store.stop();
    
    ASSERT_THROW(fut.get(), std::runtime_error);
}

// --- raft_handle_wrapper_t and is_snmg_handle Tests ---

TEST(RaftHandleWrapperTest, DetectSingleGpu) {
    raft_handle_wrapper_t wrapper(0, 0, nullptr); // device_id=0, rank=0, mg_res=nullptr
    ASSERT_FALSE(is_snmg_handle(*wrapper.get_raft_resources()));
}

/*
// Sharded mode is currently disabled due to a suspected bug in cuVS or its integration.
// In gdb output, the mdspan extents showed 18446744073709551615ul (SIZE_MAX). 
// This usually means a dynamic extent wasn't initialized correctly or a 
// calculation for the number of rows/columns overflowed/underflowed.
// Action: Check the dimensions of your input query matrix and indices. 
// If n_queries or k is being passed as a negative number or uninitialized variable, 
// cuvs might be trying to allocate a workspace based on a massive, invalid number.
TEST(RaftHandleWrapperTest, DetectMultiGpu) {
    std::vector<int> devices = {0, 1}; // Distinct devices for simulation
    auto mg_res = std::make_shared<raft::device_resources_snmg>(devices);
    init_mg_comms(*mg_res, devices);
    raft_handle_wrapper_t wrapper(0, 0, mg_res); 
    
    ASSERT_TRUE(is_snmg_handle(*wrapper.get_raft_resources()));
}
*/

// --- cuvs_worker_t Tests ---

TEST(CuvsWorkerTest, BasicLifecycle) {
    uint32_t n_threads = 1;
    cuvs_worker_t worker(n_threads, std::vector<int>{0});
    worker.start();
    worker.stop();
}

TEST(CuvsWorkerTest, SubmitTask) {
    uint32_t n_threads = 1;
    cuvs_worker_t worker(n_threads, std::vector<int>{0});
    worker.start();

    auto task = [](raft_handle_wrapper_t&) -> std::any {
        return std::string("success");
    };

    uint64_t job_id = worker.submit(task);
    auto result = worker.wait(job_id).get();

    ASSERT_EQ(std::any_cast<std::string>(result.result), std::string("success"));

    worker.stop();
}

TEST(CuvsWorkerTest, MultipleThreads) {
    uint32_t n_threads = 4;
    cuvs_worker_t worker(n_threads, std::vector<int>{0});
    worker.start();

    std::vector<uint64_t> ids;
    for (int i = 0; i < 10; ++i) {
        ids.push_back(worker.submit([i](raft_handle_wrapper_t&) -> std::any {
            return i * 2;
        }));
    }

    for (int i = 0; i < 10; ++i) {
        auto res = worker.wait(ids[i]).get();
        ASSERT_EQ(std::any_cast<int>(res.result), i * 2);
    }

    worker.stop();
}

TEST(CuvsWorkerTest, TaskErrorHandling) {
    uint32_t n_threads = 1;
    cuvs_worker_t worker(n_threads, std::vector<int>{0});
    worker.start();

    auto fail_task = [](raft_handle_wrapper_t&) -> std::any {
        throw std::runtime_error("task failed intentionally");
    };

    uint64_t job_id = worker.submit(fail_task);
    auto result = worker.wait(job_id).get();

    ASSERT_TRUE(result.error != nullptr);
    ASSERT_TRUE(has_exception<std::runtime_error>(result.error));

    worker.stop();
}

TEST(CuvsWorkerTest, SubmitMain) {
    uint32_t n_threads = 2;
    cuvs_worker_t worker(n_threads, std::vector<int>{0});
    worker.start();

    // Task that identifies the thread it's running on
    auto task = [](raft_handle_wrapper_t&) -> std::any {
        return std::this_thread::get_id();
    };

    // Submit many tasks to main to ensure they are picked up
    std::vector<uint64_t> ids;
    for(int i=0; i<10; ++i) {
        ids.push_back(worker.submit_main(task));
    }

    for(auto id : ids) {
        auto res = worker.wait(id).get();
        ASSERT_TRUE(res.error == nullptr);
    }

    worker.stop();
}

TEST(CuvsWorkerTest, BoundedQueueStress) {
    const uint32_t n_workers = 4;
    const uint32_t n_producers = 4;
    const uint32_t tasks_per_producer = 500;
    
    cuvs_worker_t worker(n_workers, std::vector<int>{0});
    worker.start();

    std::atomic<uint32_t> tasks_completed{0};
    auto task = [&](raft_handle_wrapper_t&) -> std::any {
        tasks_completed.fetch_add(1);
        // Small sleep to ensure queue builds up
        std::this_thread::sleep_for(std::chrono::microseconds(10));
        return std::any();
    };

    std::vector<std::thread> producers;
    for (uint32_t i = 0; i < n_producers; ++i) {
        producers.emplace_back([&, i]() {
            for (uint32_t j = 0; j < tasks_per_producer; ++j) {
                // Mix of submit and submit_main
                if ((i + j) % 2 == 0) {
                    worker.submit(task);
                } else {
                    worker.submit_main(task);
                }
            }
        });
    }

    for (auto& t : producers) t.join();

    // Wait for all tasks to complete (since we didn't keep track of IDs here for simplicity,
    // we just check the counter)
    const uint32_t total_tasks = n_producers * tasks_per_producer;
    auto start_time = std::chrono::steady_clock::now();
    while (tasks_completed.load() < total_tasks) {
        std::this_thread::yield();
        if (std::chrono::steady_clock::now() - start_time > std::chrono::seconds(10)) {
            REPORT_FAILURE("BoundedQueueStress timed out - possible hang");
            break;
        }
    }

    ASSERT_EQ(tasks_completed.load(), total_tasks);
    worker.stop();
}

TEST(CuvsWorkerTest, StopUnderLoad) {
    const uint32_t n_workers = 4;
    cuvs_worker_t worker(n_workers, std::vector<int>{0});
    worker.start();

    std::atomic<bool> producer_should_stop{false};
    std::thread producer([&]() {
        auto task = [](raft_handle_wrapper_t&) -> std::any {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            return std::any();
        };
        while (!producer_should_stop.load()) {
            try {
                worker.submit(task);
            } catch (const std::runtime_error& e) {
                if (std::string(e.what()) == "Worker is not running") {
                    break;
                }
                throw;
            } catch (...) {
                // Expected when worker stops
                break;
            }
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Stop the worker while tasks are being submitted/processed
    worker.stop();
    
    producer_should_stop.store(true);
    if (producer.joinable()) producer.join();
}

int main() {
    return RUN_ALL_TESTS();
}
