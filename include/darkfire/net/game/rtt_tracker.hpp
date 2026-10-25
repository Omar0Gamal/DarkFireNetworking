#pragma once

#include "../packet.hpp"

#include <chrono>
#include <cmath>
#include <cstdint>
#include <array>
#include <algorithm>

namespace darkfire::net::game {

    class RTTTracker {
    public:
        static constexpr size_t ROLLING_WINDOW_SIZE = 16;

        struct ClockSample {
            int64_t offset_us = 0;
            uint64_t rtt_us = 0;
        };

        explicit RTTTracker(double rttAlpha = 0.125, double jitterBeta = 0.25)
            : m_alpha(rttAlpha), m_beta(jitterBeta) {}

        Packet CreatePing(uint32_t pingSeq = 0) {
            Packet pkt(0, PacketFlags::HeartbeatPing);
            uint64_t t0 = GetCurrentTimeMicros();
            pkt << pingSeq << t0;
            pkt.FinalizeUDP();
            return pkt;
        }

        static Packet CreatePong(Packet& pingPacket) {
            uint64_t t1 = GetCurrentTimeMicros();
            auto reader = pingPacket.GetReader();
            uint32_t pingSeq = 0;
            uint64_t t0 = 0;
            reader >> pingSeq >> t0;

            uint64_t t2 = GetCurrentTimeMicros();

            Packet pong(0, PacketFlags::HeartbeatPong);
            pong << pingSeq << t0 << t1 << t2;
            pong.FinalizeUDP();
            return pong;
        }

        void ProcessPong(Packet& pongPacket) {
            uint64_t t3 = GetCurrentTimeMicros();

            auto reader = pongPacket.GetReader();
            uint32_t pingSeq = 0;
            uint64_t t0 = 0;
            uint64_t t1 = 0;
            uint64_t t2 = 0;

            reader >> pingSeq >> t0 >> t1 >> t2;

            if (reader.HasError()) {
                return;
            }

            AddSample(t0, t1, t2, t3);
        }

        // Direct 4-timestamp sample processing (NTP formula with outlier rejection)
        void AddSample(uint64_t t0, uint64_t t1, uint64_t t2, uint64_t t3) noexcept {
            int64_t serverProcessing = (t2 >= t1) ? static_cast<int64_t>(t2 - t1) : 0;
            int64_t totalRoundTrip = (t3 >= t0) ? static_cast<int64_t>(t3 - t0) : 0;
            int64_t sampleRttMicros = (std::max)(int64_t(0), totalRoundTrip - serverProcessing);
            double sampleRttMs = static_cast<double>(sampleRttMicros) / 1000.0;

            // 4-timestamp NTP offset formula:
            // offset = ((t1 - t0) + (t2 - t3)) / 2
            int64_t offsetMicros = ((static_cast<int64_t>(t1) - static_cast<int64_t>(t0)) +
                                    (static_cast<int64_t>(t2) - static_cast<int64_t>(t3))) / 2;

            m_lastSampleOffsetUs = offsetMicros;

            if (!m_initialized) {
                m_rttMs = sampleRttMs;
                m_jitterMs = sampleRttMs / 2.0;
                m_clockOffsetUs = offsetMicros;
                m_clockOffsetMs = static_cast<double>(offsetMicros) / 1000.0;
                m_initialized = true;

                m_window[0] = { offsetMicros, static_cast<uint64_t>(sampleRttMicros) };
                m_windowCount = 1;
                m_windowIndex = 1;
                return;
            }

            // Outlier rejection filter: check against minimum RTT in rolling window BEFORE updating stats
            uint64_t minWindowRtt = m_window[0].rtt_us;
            for (size_t i = 1; i < m_windowCount; ++i) {
                if (m_window[i].rtt_us < minWindowRtt) {
                    minWindowRtt = m_window[i].rtt_us;
                }
            }

            uint64_t rttThreshold = minWindowRtt + (std::max)(uint64_t(20000), static_cast<uint64_t>(m_jitterMs * 3000.0));
            bool isOutlier = (m_windowCount >= 4) &&
                             ((static_cast<uint64_t>(sampleRttMicros) > rttThreshold) ||
                              (static_cast<uint64_t>(sampleRttMicros) > static_cast<uint64_t>(m_rttMs * 2000.0)));

            if (!isOutlier) {
                // EWMA for RTT and Jitter
                double diff = std::abs(sampleRttMs - m_rttMs);
                m_jitterMs = m_beta * diff + (1.0 - m_beta) * m_jitterMs;
                m_rttMs = m_alpha * sampleRttMs + (1.0 - m_alpha) * m_rttMs;

                m_window[m_windowIndex] = { offsetMicros, static_cast<uint64_t>(sampleRttMicros) };
                m_windowIndex = (m_windowIndex + 1) % ROLLING_WINDOW_SIZE;
                if (m_windowCount < ROLLING_WINDOW_SIZE) {
                    m_windowCount++;
                }

                // Smooth clock offset via EWMA
                m_clockOffsetUs = static_cast<int64_t>(m_alpha * static_cast<double>(offsetMicros) + (1.0 - m_alpha) * static_cast<double>(m_clockOffsetUs));
                m_clockOffsetMs = static_cast<double>(m_clockOffsetUs) / 1000.0;
                m_consecutiveOutliers = 0;
            } else {
                m_consecutiveOutliers++;
                if (m_consecutiveOutliers >= ROLLING_WINDOW_SIZE) {
                    double diff = std::abs(sampleRttMs - m_rttMs);
                    m_jitterMs = m_beta * diff + (1.0 - m_beta) * m_jitterMs;
                    m_rttMs = m_alpha * sampleRttMs + (1.0 - m_alpha) * m_rttMs;

                    m_window[0] = { offsetMicros, static_cast<uint64_t>(sampleRttMicros) };
                    m_windowCount = 1;
                    m_windowIndex = 1;
                    m_clockOffsetUs = offsetMicros;
                    m_clockOffsetMs = static_cast<double>(offsetMicros) / 1000.0;
                    m_consecutiveOutliers = 0;
                }
            }
        }

        void add_sample(uint64_t t0, uint64_t t1, uint64_t t2, uint64_t t3) noexcept {
            AddSample(t0, t1, t2, t3);
        }

        [[nodiscard]] double GetRTT() const noexcept { return m_rttMs; }
        [[nodiscard]] double GetJitter() const noexcept { return m_jitterMs; }
        [[nodiscard]] double GetClockOffset() const noexcept { return m_clockOffsetMs; }
        [[nodiscard]] int64_t clock_offset_us() const noexcept { return m_clockOffsetUs; }
        [[nodiscard]] int64_t GetClockOffsetMicros() const noexcept { return m_clockOffsetUs; }
        [[nodiscard]] bool IsInitialized() const noexcept { return m_initialized; }

        [[nodiscard]] uint64_t estimated_server_time_us(uint64_t local_time_us) const noexcept {
            return static_cast<uint64_t>(static_cast<int64_t>(local_time_us) + m_clockOffsetUs);
        }

        [[nodiscard]] uint64_t EstimatedServerTimeMicros(uint64_t local_time_us) const noexcept {
            return estimated_server_time_us(local_time_us);
        }

        // Returns simulation pacing multiplier clamped to [0.95f, 1.05f]
        [[nodiscard]] float time_drift_scale() const noexcept {
            constexpr float MAX_ADJUSTMENT = 0.05f; // +/- 5% max adjustment
            float error_ms = static_cast<float>(m_lastSampleOffsetUs - m_clockOffsetUs) / 1000.0f;
            float adjustment = std::clamp(error_ms * 0.001f, -MAX_ADJUSTMENT, MAX_ADJUSTMENT);
            return 1.0f + adjustment;
        }

        [[nodiscard]] float TimeDriftScale() const noexcept {
            return time_drift_scale();
        }

        [[nodiscard]] uint64_t GetSyncedServerTimeMicros() const noexcept {
            return estimated_server_time_us(GetCurrentTimeMicros());
        }

        static uint64_t GetCurrentTimeMicros() noexcept {
            return static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now().time_since_epoch()
                ).count()
            );
        }

    private:
        double m_alpha;
        double m_beta;
        double m_rttMs = 0.0;
        double m_jitterMs = 0.0;
        double m_clockOffsetMs = 0.0;
        int64_t m_clockOffsetUs = 0;
        int64_t m_lastSampleOffsetUs = 0;
        bool m_initialized = false;

        std::array<ClockSample, ROLLING_WINDOW_SIZE> m_window{};
        size_t m_windowCount = 0;
        size_t m_windowIndex = 0;
        size_t m_consecutiveOutliers = 0;
    };

    using clock_sync = RTTTracker;

} // namespace darkfire::net::game
