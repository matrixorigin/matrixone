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

// --- cuvs_task_result_store_t Tests ---

TEST(CuvsTaskResultStoreTest, BasicStoreRetrieve) {
    cuvs_task_result_store_t store;
    uint64_t id = store.get_next_job_id();
    
    cuvs_task_result_t res{id, 100, nullptr};
    store.store(res);

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
        store.store({id, std::string("async"), nullptr});
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

// --- cuvs_worker_t Tests ---

TEST(CuvsWorkerTest, BasicLifecycle) {
    uint32_t n_threads = 1;
    cuvs_worker_t worker(n_threads);
    worker.start();
    worker.stop();
}

TEST(CuvsWorkerTest, SubmitTask) {
    uint32_t n_threads = 1;
    cuvs_worker_t worker(n_threads);
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
    cuvs_worker_t worker(n_threads);
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
    cuvs_worker_t worker(n_threads);
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

int main() {
    return RUN_ALL_TESTS();
}
