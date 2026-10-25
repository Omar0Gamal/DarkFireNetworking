#pragma once

#include "../packet.hpp"

#include <chrono>
#include <cstdint>
#include <random>
#include <vector>
#include <algorithm>
#include <mutex>

namespace darkfire::net::game {

    struct SimulatedPacket {
        Packet packet;
        std::chrono::steady_clock::time_point deliverTime;
    };

    class NetworkSimulator {
    public:
        NetworkSimulator() : m_rng(std::random_device{}()) {}

        void SetLatency(uint32_t minLatencyMs, uint32_t maxLatencyMs) {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_minLatencyMs = minLatencyMs;
            m_maxLatencyMs = (std::max)(minLatencyMs, maxLatencyMs);
        }

        void SetPacketLoss(double dropRate) {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_dropRate = (std::clamp)(dropRate, 0.0, 1.0);
        }

        void SetDuplicateRate(double duplicateRate) {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_duplicateRate = (std::clamp)(duplicateRate, 0.0, 1.0);
        }

        // Returns true if packet was accepted into simulator, false if dropped by packet loss
        bool Enqueue(Packet packet) {
            std::lock_guard<std::mutex> lock(m_mutex);

            // 1. Packet Loss check
            std::uniform_real_distribution<double> dist01(0.0, 1.0);
            if (dist01(m_rng) < m_dropRate) {
                m_droppedCount++;
                return false; // Packet lost!
            }

            // 2. Latency calculation
            uint32_t latencyMs = m_minLatencyMs;
            if (m_maxLatencyMs > m_minLatencyMs) {
                std::uniform_int_distribution<uint32_t> distLatency(m_minLatencyMs, m_maxLatencyMs);
                latencyMs = distLatency(m_rng);
            }

            auto now = std::chrono::steady_clock::now();
            auto deliverTime = now + std::chrono::milliseconds(latencyMs);

            m_queue.push_back({ packet, deliverTime });
            m_sentCount++;

            // 3. Optional duplicate simulation
            if (dist01(m_rng) < m_duplicateRate) {
                auto dupTime = deliverTime + std::chrono::milliseconds(5);
                m_queue.push_back({ packet, dupTime });
                m_sentCount++;
            }

            return true;
        }

        // Returns all packets whose delivery time has elapsed
        std::vector<Packet> PollReady() {
            std::lock_guard<std::mutex> lock(m_mutex);
            auto now = std::chrono::steady_clock::now();

            std::vector<Packet> ready;
            auto it = m_queue.begin();
            while (it != m_queue.end()) {
                if (now >= it->deliverTime) {
                    ready.push_back(std::move(it->packet));
                    it = m_queue.erase(it);
                } else {
                    ++it;
                }
            }

            return ready;
        }

        [[nodiscard]] size_t PendingCount() const {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_queue.size();
        }

        [[nodiscard]] uint64_t DroppedCount() const noexcept { return m_droppedCount; }
        [[nodiscard]] uint64_t SentCount() const noexcept { return m_sentCount; }

        void Clear() {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_queue.clear();
        }

    private:
        mutable std::mutex m_mutex;
        std::mt19937 m_rng;
        uint32_t m_minLatencyMs = 0;
        uint32_t m_maxLatencyMs = 0;
        double m_dropRate = 0.0;
        double m_duplicateRate = 0.0;
        uint64_t m_droppedCount = 0;
        uint64_t m_sentCount = 0;
        std::vector<SimulatedPacket> m_queue;
    };

} // namespace darkfire::net::game
