#pragma once

#include "error.hpp"
#include "endian.hpp"
#include "buffer_view.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace darkfire::net {

    class ByteReader {
    public:
        constexpr ByteReader() noexcept : m_span(), m_cursor(0), m_lastError(NetErrorCode::Success) {}
        
        constexpr explicit ByteReader(std::span<const uint8_t> span) noexcept
            : m_span(span), m_cursor(0), m_lastError(NetErrorCode::Success) {}

        explicit ByteReader(const uint8_t* data, size_t size) noexcept
            : m_span(data, size), m_cursor(0), m_lastError(NetErrorCode::Success) {}

        explicit ByteReader(const std::vector<uint8_t>& vec) noexcept
            : m_span(vec.data(), vec.size()), m_cursor(0), m_lastError(NetErrorCode::Success) {}

        explicit ByteReader(BufferView view) noexcept
            : m_span(view.AsSpan()), m_cursor(0), m_lastError(NetErrorCode::Success) {}

        [[nodiscard]] constexpr size_t TotalSize() const noexcept { return m_span.size(); }
        [[nodiscard]] constexpr size_t Cursor() const noexcept { return m_cursor; }
        [[nodiscard]] constexpr size_t Remaining() const noexcept { 
            return (m_cursor < m_span.size()) ? (m_span.size() - m_cursor) : 0; 
        }
        [[nodiscard]] constexpr bool HasBytes(size_t n) const noexcept { return Remaining() >= n; }
        [[nodiscard]] constexpr bool Empty() const noexcept { return Remaining() == 0; }
        [[nodiscard]] constexpr bool IsOk() const noexcept { return m_lastError == NetErrorCode::Success; }
        [[nodiscard]] constexpr bool HasError() const noexcept { return m_lastError != NetErrorCode::Success; }
        [[nodiscard]] constexpr NetErrorCode LastError() const noexcept { return m_lastError; }

        void Reset() noexcept {
            m_cursor = 0;
            m_lastError = NetErrorCode::Success;
        }

        NetResult<void> Seek(size_t position) noexcept {
            if (position > m_span.size()) {
                m_lastError = NetErrorCode::BufferOverflow;
                return m_lastError;
            }
            m_cursor = position;
            return NetResult<void>();
        }

        NetResult<void> Skip(size_t count) noexcept {
            if (!HasBytes(count)) {
                m_lastError = NetErrorCode::BufferUnderflow;
                return m_lastError;
            }
            m_cursor += count;
            return NetResult<void>();
        }

        // Primitives & Scalars
        template <SerializableScalar T>
        NetResult<T> Read() noexcept {
            if (!HasBytes(sizeof(T))) {
                m_lastError = NetErrorCode::BufferUnderflow;
                return m_lastError;
            }

            T netVal{};
            std::memcpy(&netVal, m_span.data() + m_cursor, sizeof(T));
            m_cursor += sizeof(T);
            return NetworkToHost(netVal);
        }

        NetResult<uint8_t>  ReadUInt8()  noexcept { return Read<uint8_t>(); }
        NetResult<uint16_t> ReadUInt16() noexcept { return Read<uint16_t>(); }
        NetResult<uint32_t> ReadUInt32() noexcept { return Read<uint32_t>(); }
        NetResult<uint64_t> ReadUInt64() noexcept { return Read<uint64_t>(); }
        NetResult<int8_t>   ReadInt8()   noexcept { return Read<int8_t>(); }
        NetResult<int16_t>  ReadInt16()  noexcept { return Read<int16_t>(); }
        NetResult<int32_t>  ReadInt32()  noexcept { return Read<int32_t>(); }
        NetResult<int64_t>  ReadInt64()  noexcept { return Read<int64_t>(); }
        NetResult<float>    ReadFloat()  noexcept { return Read<float>(); }
        NetResult<double>   ReadDouble() noexcept { return Read<double>(); }
        
        NetResult<bool> ReadBool() noexcept {
            auto b = ReadUInt8();
            if (!b) return b.Error();
            return *b != 0;
        }

        // Enums
        template <SerializableEnum E>
        NetResult<E> ReadEnum() noexcept {
            using Underlying = std::underlying_type_t<E>;
            auto res = Read<Underlying>();
            if (!res) return res.Error();
            return static_cast<E>(*res);
        }

        // Strings & String Views (Zero-Copy)
        NetResult<std::string_view> ReadStringView() noexcept {
            auto lenRes = ReadUInt32();
            if (!lenRes) return lenRes.Error();

            uint32_t length = *lenRes;
            if (!HasBytes(length)) {
                m_lastError = NetErrorCode::BufferUnderflow;
                return m_lastError;
            }

            std::string_view sv(reinterpret_cast<const char*>(m_span.data() + m_cursor), length);
            m_cursor += length;
            return sv;
        }

        NetResult<std::string> ReadString() noexcept {
            auto sv = ReadStringView();
            if (!sv) return sv.Error();
            return std::string(*sv);
        }

        // Raw bytes (Zero-Copy)
        NetResult<std::span<const uint8_t>> ReadBytes(size_t count) noexcept {
            if (!HasBytes(count)) {
                m_lastError = NetErrorCode::BufferUnderflow;
                return m_lastError;
            }

            std::span<const uint8_t> slice(m_span.data() + m_cursor, count);
            m_cursor += count;
            return slice;
        }

        // Dynamic vectors
        template <typename T>
        NetResult<std::vector<T>> ReadVector() {
            auto countRes = ReadUInt32();
            if (!countRes) return countRes.Error();

            uint32_t count = *countRes;
            if (count > Remaining()) {
                m_lastError = NetErrorCode::BufferUnderflow;
                return m_lastError;
            }

            std::vector<T> vec;
            vec.reserve(count);
            for (uint32_t i = 0; i < count; ++i) {
                T item{};
                *this >> item;
                if (HasError()) {
                    return m_lastError;
                }
                vec.push_back(std::move(item));
            }
            return vec;
        }

        // Trivially copyable structs
        template <TriviallySerializable T>
        NetResult<T> ReadStruct() noexcept {
            if (!HasBytes(sizeof(T))) {
                m_lastError = NetErrorCode::BufferUnderflow;
                return m_lastError;
            }

            T obj{};
            std::memcpy(&obj, m_span.data() + m_cursor, sizeof(T));
            m_cursor += sizeof(T);
            return obj;
        }

        // Streaming operator>>
        template <SerializableScalar T>
        friend ByteReader& operator>>(ByteReader& reader, T& value) {
            if (reader.HasError()) {
                value = T{};
                return reader;
            }
            auto res = reader.Read<T>();
            if (res) {
                value = *res;
            } else {
                value = T{};
            }
            return reader;
        }

        template <SerializableEnum E>
        friend ByteReader& operator>>(ByteReader& reader, E& value) {
            if (reader.HasError()) {
                value = static_cast<E>(0);
                return reader;
            }
            auto res = reader.ReadEnum<E>();
            if (res) {
                value = *res;
            } else {
                value = static_cast<E>(0);
            }
            return reader;
        }

        friend ByteReader& operator>>(ByteReader& reader, std::string& value) {
            if (reader.HasError()) {
                value.clear();
                return reader;
            }
            auto res = reader.ReadString();
            if (res) {
                value = std::move(*res);
            } else {
                value.clear();
            }
            return reader;
        }

        friend ByteReader& operator>>(ByteReader& reader, bool& value) {
            if (reader.HasError()) {
                value = false;
                return reader;
            }
            auto res = reader.ReadBool();
            if (res) {
                value = *res;
            } else {
                value = false;
            }
            return reader;
        }

        template <typename T>
        friend ByteReader& operator>>(ByteReader& reader, std::vector<T>& vec) {
            if (reader.HasError()) {
                vec.clear();
                return reader;
            }
            auto res = reader.ReadVector<T>();
            if (res) {
                vec = std::move(*res);
            } else {
                vec.clear();
            }
            return reader;
        }

        template <TriviallySerializable T>
        friend ByteReader& operator>>(ByteReader& reader, T& value) {
            if (reader.HasError()) {
                value = T{};
                return reader;
            }
            auto res = reader.ReadStruct<T>();
            if (res) {
                value = *res;
            } else {
                value = T{};
            }
            return reader;
        }

    private:
        std::span<const uint8_t> m_span;
        size_t m_cursor = 0;
        NetErrorCode m_lastError = NetErrorCode::Success;
    };

} // namespace darkfire::net
