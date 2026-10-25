#include <darkfire/darkfire.hpp>
#include <cassert>
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>

void TestSPSCSingleThread() {
    std::cout << "  [TEST] SPSCQueue Single-Threaded Boundary Checks... ";

    darkfire::net::SPSCQueue<int, 4> queue;
    assert(queue.Empty());
    assert(queue.Size() == 0);

    // Push 4 items (capacity 4)
    assert(queue.Push(10));
    assert(queue.Push(20));
    assert(queue.Push(30));
    assert(queue.Push(40));
    assert(!queue.Push(50)); // Full

    int val = 0;
    assert(queue.Pop(val) && val == 10);
    assert(queue.Pop(val) && val == 20);
    assert(queue.Pop(val) && val == 30);
    assert(queue.Pop(val) && val == 40);
    assert(!queue.Pop(val)); // Empty now

    std::cout << "PASSED\n";
}

void TestSPSCMultiThreaded() {
    std::cout << "  [TEST] SPSCQueue Multi-Threaded Concurrent Ping-Pong... ";

    constexpr int TotalItems = 50000;
    darkfire::net::SPSCQueue<int, 1024> queue;
    std::atomic<bool> producerDone{ false };

    std::thread producer([&]() {
        for (int i = 1; i <= TotalItems; ++i) {
            while (!queue.Push(i)) {
                std::this_thread::yield();
            }
        }
        producerDone.store(true, std::memory_order_release);
    });

    std::thread consumer([&]() {
        int expected = 1;
        while (!producerDone.load(std::memory_order_acquire) || !queue.Empty()) {
            int val = 0;
            if (queue.Pop(val)) {
                assert(val == expected);
                expected++;
            } else {
                std::this_thread::yield();
            }
        }
        assert(expected == TotalItems + 1);
    });

    producer.join();
    consumer.join();

    std::cout << "PASSED (" << TotalItems << " items verified)\n";
}

void TestMPSCMultiProducer() {
    std::cout << "  [TEST] MPSCQueue Multi-Producer Single-Consumer... ";

    constexpr int Producers = 4;
    constexpr int ItemsPerProducer = 10000;
    darkfire::net::MPSCQueue<int> queue;
    std::atomic<int> producersFinished{ 0 };

    std::vector<std::thread> threads;
    for (int p = 0; p < Producers; ++p) {
        threads.emplace_back([&, p]() {
            for (int i = 0; i < ItemsPerProducer; ++i) {
                queue.Push(p * 100000 + i);
            }
            producersFinished.fetch_add(1, std::memory_order_release);
        });
    }

    int receivedCount = 0;
    while (producersFinished.load(std::memory_order_acquire) < Producers || !queue.Empty()) {
        int val = 0;
        if (queue.Pop(val)) {
            receivedCount++;
        } else {
            std::this_thread::yield();
        }
    }

    for (auto& t : threads) {
        t.join();
    }

    assert(receivedCount == Producers * ItemsPerProducer);
    std::cout << "PASSED (" << receivedCount << " items processed across 4 producers)\n";
}

void RunLockFreeQueueTests() {
    std::cout << "\n--- Running Lock-Free Queue Tests ---\n";
    TestSPSCSingleThread();
    TestSPSCMultiThreaded();
    TestMPSCMultiProducer();
}
