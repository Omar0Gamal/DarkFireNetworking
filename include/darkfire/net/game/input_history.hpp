#pragma once

#include "../byte_writer.hpp"
#include "../byte_reader.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <algorithm>
#include <concepts>
#include <type_traits>
#include <cstring>

namespace darkfire::net::game {

    // Helper serialization functors for TInput types
    namespace detail {
        template <typename Writer, typename T>
        void write_input_element(Writer& writer, const T& item) {
            if constexpr (requires { writer << item; }) {
                writer << item;
            } else if constexpr (requires { item.serialize(writer); }) {
                item.serialize(writer);
            } else if constexpr (std::is_trivially_copyable_v<T>) {
                writer.WriteBytes(&item, sizeof(T));
            } else {
                static_assert(sizeof(T) == 0, "TInput must be serializable via operator<<, .serialize(writer), or be trivially copyable");
            }
        }

        template <typename Reader, typename T>
        bool read_input_element(Reader& reader, T& item) {
            if constexpr (requires { reader >> item; }) {
                reader >> item;
                return !reader.HasError();
            } else if constexpr (requires { item.deserialize(reader); }) {
                item.deserialize(reader);
                return !reader.HasError();
            } else if constexpr (std::is_trivially_copyable_v<T>) {
                auto slice = reader.ReadBytes(sizeof(T));
                if (!slice) return false;
                std::memcpy(&item, slice->data(), sizeof(T));
                return true;
            } else {
                static_assert(sizeof(T) == 0, "TInput must be deserializable via operator>>, .deserialize(reader), or be trivially copyable");
                return false;
            }
        }
    } // namespace detail

    template <typename TInput, typename TState>
    struct input_entry {
        uint32_t tick = 0;
        TInput input{};
        TState predicted_state{};
    };

    template <typename TInput, typename TState>
    using InputEntry = input_entry<TInput, TState>;

    // Forward declaration of redundant_input_bundle
    template <typename TInput, size_t RedundancyCount>
    class redundant_input_bundle;

    // Templated ring buffer for local client prediction & rollback reconciliation
    template <typename TInput, typename TState, size_t Capacity = 128>
    class input_ring_buffer {
        static_assert(Capacity >= 2, "Capacity must be at least 2");

    public:
        input_ring_buffer() = default;

        void push(uint32_t tick, const TInput& input, const TState& predicted_state) {
            size_t idx = tick % Capacity;
            m_buffer[idx] = { tick, input, predicted_state };

            if (m_count == 0) {
                m_oldestTick = tick;
                m_newestTick = tick;
                m_count = 1;
            } else if (tick > m_newestTick) {
                m_newestTick = tick;
                if (m_count < Capacity) {
                    m_count++;
                } else {
                    m_oldestTick = m_newestTick - static_cast<uint32_t>(Capacity) + 1;
                }
            } else if (tick < m_oldestTick) {
                m_oldestTick = tick;
                if (m_count < Capacity) {
                    m_count++;
                }
            }
        }

        void Push(uint32_t tick, const TInput& input, const TState& predicted_state) {
            push(tick, input, predicted_state);
        }

        [[nodiscard]] const input_entry<TInput, TState>* find(uint32_t tick) const noexcept {
            if (m_count == 0 || tick < m_oldestTick || tick > m_newestTick) {
                return nullptr;
            }
            const auto& entry = m_buffer[tick % Capacity];
            return (entry.tick == tick) ? &entry : nullptr;
        }

        [[nodiscard]] input_entry<TInput, TState>* find(uint32_t tick) noexcept {
            if (m_count == 0 || tick < m_oldestTick || tick > m_newestTick) {
                return nullptr;
            }
            auto& entry = m_buffer[tick % Capacity];
            return (entry.tick == tick) ? &entry : nullptr;
        }

        [[nodiscard]] const input_entry<TInput, TState>* Find(uint32_t tick) const noexcept { return find(tick); }
        [[nodiscard]] input_entry<TInput, TState>* Find(uint32_t tick) noexcept { return find(tick); }

        [[nodiscard]] std::optional<input_entry<TInput, TState>> get(uint32_t tick) const noexcept {
            const auto* ptr = find(tick);
            if (!ptr) return std::nullopt;
            return *ptr;
        }

        [[nodiscard]] std::optional<input_entry<TInput, TState>> Get(uint32_t tick) const noexcept { return get(tick); }

        [[nodiscard]] bool has(uint32_t tick) const noexcept {
            return find(tick) != nullptr;
        }

        [[nodiscard]] bool Has(uint32_t tick) const noexcept { return has(tick); }

        bool update_predicted_state(uint32_t tick, const TState& state) noexcept {
            auto* entry = find(tick);
            if (!entry) return false;
            entry->predicted_state = state;
            return true;
        }

        bool UpdatePredictedState(uint32_t tick, const TState& state) noexcept {
            return update_predicted_state(tick, state);
        }

        // Acknowledge up to tick (confirmed by server, discarding older history)
        void acknowledge_up_to(uint32_t acked_tick) noexcept {
            if (m_count == 0) return;
            if (acked_tick >= m_newestTick) {
                m_oldestTick = m_newestTick + 1;
                m_count = 0;
            } else if (acked_tick >= m_oldestTick) {
                m_oldestTick = acked_tick + 1;
                m_count = static_cast<size_t>(m_newestTick - m_oldestTick + 1);
            }
        }

        void AcknowledgeUpTo(uint32_t acked_tick) noexcept {
            acknowledge_up_to(acked_tick);
        }

        [[nodiscard]] std::optional<input_entry<TInput, TState>> get_newest() const noexcept {
            if (m_count == 0) return std::nullopt;
            return get(m_newestTick);
        }

        [[nodiscard]] std::optional<input_entry<TInput, TState>> get_oldest() const noexcept {
            if (m_count == 0) return std::nullopt;
            return get(m_oldestTick);
        }

        [[nodiscard]] uint32_t latest_tick() const noexcept { return m_newestTick; }
        [[nodiscard]] uint32_t oldest_tick() const noexcept { return m_oldestTick; }
        [[nodiscard]] uint32_t LatestTick() const noexcept { return m_newestTick; }
        [[nodiscard]] uint32_t OldestTick() const noexcept { return m_oldestTick; }

        [[nodiscard]] size_t size() const noexcept { return m_count; }
        [[nodiscard]] size_t Size() const noexcept { return m_count; }
        [[nodiscard]] constexpr size_t capacity() const noexcept { return Capacity; }
        [[nodiscard]] constexpr size_t CapacityLimit() const noexcept { return Capacity; }
        [[nodiscard]] bool empty() const noexcept { return m_count == 0; }
        [[nodiscard]] bool Empty() const noexcept { return m_count == 0; }

        void clear() noexcept {
            m_count = 0;
            m_oldestTick = 0;
            m_newestTick = 0;
        }

        void Clear() noexcept { clear(); }

        template <size_t RedundancyCount = 4>
        redundant_input_bundle<TInput, RedundancyCount> build_bundle(uint32_t target_tick) const;

    private:
        std::array<input_entry<TInput, TState>, Capacity> m_buffer{};
        uint32_t m_oldestTick = 0;
        uint32_t m_newestTick = 0;
        size_t m_count = 0;
    };

    template <typename TInput, typename TState, size_t Capacity = 128>
    using InputRingBuffer = input_ring_buffer<TInput, TState, Capacity>;

    // Redundant input wire bundle for recovering dropped UDP packets
    template <typename TInput, size_t RedundancyCount = 4>
    class redundant_input_bundle {
        static_assert(RedundancyCount >= 1, "RedundancyCount must be at least 1");

    public:
        uint32_t latest_tick = 0;
        uint8_t count = 0; // Number of valid historical inputs in bundle (1..RedundancyCount)
        std::array<TInput, RedundancyCount> inputs{};

        redundant_input_bundle() = default;

        [[nodiscard]] uint32_t oldest_tick() const noexcept {
            return (count > 0 && latest_tick + 1 >= count) ? (latest_tick - static_cast<uint32_t>(count) + 1) : 0;
        }

        [[nodiscard]] uint32_t OldestTick() const noexcept { return oldest_tick(); }
        [[nodiscard]] uint32_t LatestTick() const noexcept { return latest_tick; }

        [[nodiscard]] bool has_input_for_tick(uint32_t tick) const noexcept {
            return count > 0 && tick >= oldest_tick() && tick <= latest_tick;
        }

        [[nodiscard]] bool HasInputForTick(uint32_t tick) const noexcept {
            return has_input_for_tick(tick);
        }

        [[nodiscard]] std::optional<TInput> get_input_for_tick(uint32_t tick) const noexcept {
            if (!has_input_for_tick(tick)) return std::nullopt;
            size_t idx = static_cast<size_t>(tick - oldest_tick());
            return inputs[idx];
        }

        [[nodiscard]] std::optional<TInput> GetInputForTick(uint32_t tick) const noexcept {
            return get_input_for_tick(tick);
        }

        bool get_input(uint32_t tick, TInput& out_input) const noexcept {
            if (!has_input_for_tick(tick)) return false;
            out_input = inputs[tick - oldest_tick()];
            return true;
        }

        void add_input(uint32_t tick, const TInput& input) {
            if (count == 0) {
                latest_tick = tick;
                inputs[0] = input;
                count = 1;
            } else if (tick == latest_tick + 1) {
                if (count < RedundancyCount) {
                    inputs[count] = input;
                    count++;
                } else {
                    for (size_t i = 0; i < RedundancyCount - 1; ++i) {
                        inputs[i] = inputs[i + 1];
                    }
                    inputs[RedundancyCount - 1] = input;
                }
                latest_tick = tick;
            } else if (has_input_for_tick(tick)) {
                inputs[tick - oldest_tick()] = input;
            } else if (tick > latest_tick) {
                // Gap jump: reset with current input
                latest_tick = tick;
                inputs[0] = input;
                count = 1;
            }
        }

        void AddInput(uint32_t tick, const TInput& input) {
            add_input(tick, input);
        }

        template <typename TState, size_t Cap>
        static redundant_input_bundle build(const input_ring_buffer<TInput, TState, Cap>& buffer, uint32_t target_tick) {
            redundant_input_bundle bundle;
            if (buffer.empty() || !buffer.has(target_tick)) {
                return bundle;
            }

            uint32_t start_tick = (target_tick >= RedundancyCount - 1) ? (target_tick - static_cast<uint32_t>(RedundancyCount) + 1) : 0;
            if (start_tick < buffer.oldest_tick()) {
                start_tick = buffer.oldest_tick();
            }

            for (uint32_t t = start_tick; t <= target_tick; ++t) {
                const auto* entry = buffer.find(t);
                if (entry) {
                    bundle.add_input(t, entry->input);
                }
            }
            return bundle;
        }

        void serialize(ByteWriter& writer) const {
            writer.WriteUInt32(latest_tick);
            writer.WriteUInt8(count);
            for (size_t i = 0; i < count; ++i) {
                detail::write_input_element(writer, inputs[i]);
            }
        }

        bool deserialize(ByteReader& reader) {
            auto resTick = reader.ReadUInt32();
            auto resCount = reader.ReadUInt8();
            if (!resTick || !resCount) return false;

            latest_tick = *resTick;
            uint8_t c = *resCount;
            if (c > RedundancyCount) return false;

            count = c;
            for (size_t i = 0; i < count; ++i) {
                if (!detail::read_input_element(reader, inputs[i])) {
                    return false;
                }
            }
            return true;
        }

        friend ByteWriter& operator<<(ByteWriter& writer, const redundant_input_bundle& bundle) {
            bundle.serialize(writer);
            return writer;
        }

        friend ByteReader& operator>>(ByteReader& reader, redundant_input_bundle& bundle) {
            bundle.deserialize(reader);
            return reader;
        }
    };

    template <typename TInput, size_t RedundancyCount = 4>
    using RedundantInputBundle = redundant_input_bundle<TInput, RedundancyCount>;

    template <typename TInput, typename TState, size_t Capacity>
    template <size_t RedundancyCount>
    redundant_input_bundle<TInput, RedundancyCount> input_ring_buffer<TInput, TState, Capacity>::build_bundle(uint32_t target_tick) const {
        return redundant_input_bundle<TInput, RedundancyCount>::build(*this, target_tick);
    }

} // namespace darkfire::net::game