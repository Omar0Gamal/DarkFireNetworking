#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <algorithm>
#include <type_traits>

namespace darkfire::net::game {

    namespace detail {
        template <typename Val, typename Scale>
        constexpr auto scale_vector_val(const Val& v, Scale s) {
            if constexpr (requires { v * s; }) {
                return v * s;
            } else if constexpr (requires { s * v; }) {
                return s * v;
            } else {
                static_assert(sizeof(Val) == 0, "Type must support scalar multiplication");
            }
        }
    } // namespace detail

    template <typename T>
    struct Snapshot {
        uint64_t timestamp = 0; // Microseconds or milliseconds
        T state{};
        uint32_t last_acknowledged_input_tick = 0;
    };

    template <typename T>
    using snapshot_entry = Snapshot<T>;

    template <typename T, size_t BufferSize = 64>
    class SnapshotHistory {
        static_assert(BufferSize >= 2, "BufferSize must be at least 2");

    public:
        SnapshotHistory() = default;

        explicit SnapshotHistory(float timestampScaleToSec)
            : m_timestampScale(timestampScaleToSec) {}

        void AddSnapshot(uint64_t timestamp, const T& state, uint32_t last_acknowledged_input_tick = 0) {
            size_t idx = (m_head + 1) % BufferSize;
            m_buffer[idx] = { timestamp, state, last_acknowledged_input_tick };
            m_head = idx;
            if (m_count < BufferSize) {
                m_count++;
            }
        }

        void add_snapshot(uint64_t timestamp, const T& state, uint32_t last_acknowledged_input_tick = 0) {
            AddSnapshot(timestamp, state, last_acknowledged_input_tick);
        }

        [[nodiscard]] size_t Count() const noexcept { return m_count; }
        [[nodiscard]] size_t size() const noexcept { return m_count; }
        [[nodiscard]] bool Empty() const noexcept { return m_count == 0; }
        [[nodiscard]] bool empty() const noexcept { return m_count == 0; }

        void Clear() noexcept {
            m_count = 0;
            m_head = 0;
        }

        void clear() noexcept { Clear(); }

        [[nodiscard]] std::optional<Snapshot<T>> GetNewest() const noexcept {
            if (m_count == 0) return std::nullopt;
            return m_buffer[m_head];
        }

        [[nodiscard]] std::optional<Snapshot<T>> get_newest() const noexcept {
            return GetNewest();
        }

        [[nodiscard]] std::optional<Snapshot<T>> GetOldest() const noexcept {
            if (m_count == 0) return std::nullopt;
            size_t oldestIdx = (m_head + BufferSize - m_count + 1) % BufferSize;
            return m_buffer[oldestIdx];
        }

        [[nodiscard]] std::optional<Snapshot<T>> get_oldest() const noexcept {
            return GetOldest();
        }

        [[nodiscard]] uint32_t last_acknowledged_input_tick() const noexcept {
            if (m_count == 0) return 0;
            return m_buffer[m_head].last_acknowledged_input_tick;
        }

        [[nodiscard]] uint32_t LastAcknowledgedInputTick() const noexcept {
            return last_acknowledged_input_tick();
        }

        void set_timestamp_scale(float scale) noexcept { m_timestampScale = scale; }
        void SetTimestampScale(float scale) noexcept { m_timestampScale = scale; }
        [[nodiscard]] float timestamp_scale() const noexcept { return m_timestampScale; }

        // Generic Hermite Spline calculation utility:
        // P(t) = h00*p0 + h10*v0*dt + h01*p1 + h11*v1*dt
        template <typename TPos, typename TVel>
        static constexpr auto hermite_interpolate(
            const TPos& p0,
            const TVel& v0,
            const TPos& p1,
            const TVel& v1,
            float t,
            float dt_sec
        ) {
            float t2 = t * t;
            float t3 = t2 * t;

            float h00 = 2.0f * t3 - 3.0f * t2 + 1.0f;
            float h10 = t3 - 2.0f * t2 + t;
            float h01 = -2.0f * t3 + 3.0f * t2;
            float h11 = t3 - t2;

            auto term0 = detail::scale_vector_val(p0, h00);
            auto term1 = detail::scale_vector_val(v0, h10 * dt_sec);
            auto term2 = detail::scale_vector_val(p1, h01);
            auto term3 = detail::scale_vector_val(v1, h11 * dt_sec);

            return term0 + term1 + term2 + term3;
        }

        // Sampling with user-supplied callable:
        // callable can be interp_fn(const T& a, const T& b, float t, float dt_sec)
        // or lerp_fn(const T& a, const T& b, double t)
        template <typename InterpFunc>
        bool sample(uint64_t render_timestamp, T& out_state, InterpFunc&& interp_fn, float custom_dt_scale = 0.0f) const {
            if (m_count == 0) {
                return false;
            }

            if (m_count == 1) {
                out_state = m_buffer[m_head].state;
                return true;
            }

            auto newest = m_buffer[m_head];
            size_t oldestIdx = (m_head + BufferSize - m_count + 1) % BufferSize;
            auto oldest = m_buffer[oldestIdx];

            if (render_timestamp >= newest.timestamp) {
                out_state = newest.state;
                return true;
            }
            if (render_timestamp <= oldest.timestamp) {
                out_state = oldest.state;
                return true;
            }

            float dt_scale = (custom_dt_scale > 0.0f) ? custom_dt_scale : m_timestampScale;

            for (size_t i = 0; i < m_count - 1; ++i) {
                size_t currIdx = (m_head + BufferSize - i) % BufferSize;
                size_t prevIdx = (currIdx + BufferSize - 1) % BufferSize;

                const auto& s1 = m_buffer[currIdx]; // newer
                const auto& s0 = m_buffer[prevIdx]; // older

                if (render_timestamp >= s0.timestamp && render_timestamp <= s1.timestamp) {
                    if (s1.timestamp == s0.timestamp) {
                        out_state = s1.state;
                        return true;
                    }

                    float t = static_cast<float>(static_cast<double>(render_timestamp - s0.timestamp) /
                                                 static_cast<double>(s1.timestamp - s0.timestamp));
                    t = std::clamp(t, 0.0f, 1.0f);
                    float dt_sec = static_cast<float>(s1.timestamp - s0.timestamp) * dt_scale;

                    if constexpr (std::is_invocable_v<InterpFunc, const T&, const T&, float, float>) {
                        out_state = interp_fn(s0.state, s1.state, t, dt_sec);
                    } else if constexpr (std::is_invocable_v<InterpFunc, const T&, const T&, double>) {
                        out_state = interp_fn(s0.state, s1.state, static_cast<double>(t));
                    } else {
                        out_state = interp_fn(s0.state, s1.state, t);
                    }
                    return true;
                }
            }

            out_state = newest.state;
            return true;
        }

        template <typename LerpFunc>
        std::optional<T> Sample(uint64_t targetTime, LerpFunc&& lerp) const {
            T state{};
            if (sample(targetTime, state, std::forward<LerpFunc>(lerp))) {
                return state;
            }
            return std::nullopt;
        }

    private:
        std::array<Snapshot<T>, BufferSize> m_buffer{};
        size_t m_head = 0;
        size_t m_count = 0;
        float m_timestampScale = 0.001f; // Default: ms to seconds (0.001)
    };

    template <typename T, size_t BufferSize = 64>
    using snapshot_history = SnapshotHistory<T, BufferSize>;

    // Standalone / free function Hermite spline helper
    template <typename TPos, typename TVel>
    constexpr auto hermite_interpolate(
        const TPos& p0,
        const TVel& v0,
        const TPos& p1,
        const TVel& v1,
        float t,
        float dt_sec
    ) {
        return SnapshotHistory<int>::hermite_interpolate(p0, v0, p1, v1, t, dt_sec);
    }

} // namespace darkfire::net::game