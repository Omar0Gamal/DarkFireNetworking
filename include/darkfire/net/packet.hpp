#pragma once

#include "error.hpp"
#include "endian.hpp"
#include "security.hpp"
#include "byte_writer.hpp"
#include "byte_reader.hpp"
#include "buffer_view.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

namespace darkfire::net {

    // Global limits
    constexpr size_t MAX_PACKET_SIZE = 65536;       // 64 KB maximum payload
    constexpr size_t STANDARD_MTU = 1200;           // Standard safe UDP datagram payload limit
    constexpr uint16_t MAGIC_TCP = 0xDF4E;          // 'DF' 'N'
    constexpr uint16_t MAGIC_UDP = 0xDF55;          // 'DF' 'U'
    constexpr uint8_t PROTOCOL_VERSION = 2;

    namespace PacketFlags {
        constexpr uint8_t None               = 0x00;
        constexpr uint8_t HandshakeChallenge = 0x01;
        constexpr uint8_t HandshakeResponse  = 0x02;
        constexpr uint8_t HeartbeatPing      = 0x04;
        constexpr uint8_t HeartbeatPong      = 0x08;
        constexpr uint8_t Reliable           = 0x10;
        constexpr uint8_t Fragmented         = 0x20;
        constexpr uint8_t SystemMessage      = 0x40;
        constexpr uint8_t Sequenced          = 0x80;
        constexpr uint8_t UnreliableSequenced = 0x80;
    }

    // Modern naming aliases
    using byte_writer = ByteWriter;
    using byte_reader = ByteReader;

    // RFC 1982 Serial Number Arithmetic for wrap-around safe sequence comparisons
    template <typename UInt>
        requires (std::is_unsigned_v<UInt> && (sizeof(UInt) == 2 || sizeof(UInt) == 4))
    [[nodiscard]] constexpr bool sequence_greater_than(UInt s1, UInt s2) noexcept {
        constexpr UInt half_range = static_cast<UInt>(UInt(1) << (sizeof(UInt) * 8 - 1));
        return ((s1 > s2) && (static_cast<UInt>(s1 - s2) < half_range)) ||
               ((s1 < s2) && (static_cast<UInt>(s2 - s1) > half_range));
    }

    template <typename UInt>
        requires (std::is_unsigned_v<UInt> && (sizeof(UInt) == 2 || sizeof(UInt) == 4))
    [[nodiscard]] constexpr bool sequence_less_than(UInt s1, UInt s2) noexcept {
        return (s1 != s2) && !sequence_greater_than(s1, s2);
    }

    template <typename UInt>
        requires (std::is_unsigned_v<UInt> && (sizeof(UInt) == 2 || sizeof(UInt) == 4))
    [[nodiscard]] constexpr bool sequence_greater_or_equal(UInt s1, UInt s2) noexcept {
        return (s1 == s2) || sequence_greater_than(s1, s2);
    }

    template <typename UInt>
        requires (std::is_unsigned_v<UInt> && (sizeof(UInt) == 2 || sizeof(UInt) == 4))
    [[nodiscard]] constexpr bool sequence_less_or_equal(UInt s1, UInt s2) noexcept {
        return (s1 == s2) || sequence_less_than(s1, s2);
    }

#pragma pack(push, 1)
    struct TCPHeader {
        uint16_t magic = HostToNetwork(MAGIC_TCP);
        uint8_t version = PROTOCOL_VERSION;
        uint8_t flags = PacketFlags::None;
        uint16_t message_id = 0;
        uint32_t body_size = 0;
        uint32_t checksum = 0; // CRC32 of payload

        [[nodiscard]] uint16_t GetMagic() const noexcept { return NetworkToHost(magic); }
        [[nodiscard]] uint16_t GetMessageId() const noexcept { return NetworkToHost(message_id); }
        [[nodiscard]] uint32_t GetBodySize() const noexcept { return NetworkToHost(body_size); }
        [[nodiscard]] uint32_t GetChecksum() const noexcept { return NetworkToHost(checksum); }

        void SetMagic(uint16_t m) noexcept { magic = HostToNetwork(m); }
        void SetMessageId(uint16_t id) noexcept { message_id = HostToNetwork(id); }
        void SetBodySize(uint32_t size) noexcept { body_size = HostToNetwork(size); }
        void SetChecksum(uint32_t c) noexcept { checksum = HostToNetwork(c); }

        [[nodiscard]] bool IsValidMagic() const noexcept {
            return GetMagic() == MAGIC_TCP;
        }

        [[nodiscard]] bool IsValidSize() const noexcept {
            return GetBodySize() <= MAX_PACKET_SIZE;
        }
    };

    struct UDPHeader {
        uint16_t magic = HostToNetwork(MAGIC_UDP);
        uint8_t version = PROTOCOL_VERSION;
        uint8_t flags = PacketFlags::None;
        uint16_t message_id = 0;
        uint32_t sequence_id = 0;
        uint32_t ack_id = 0;
        uint32_t ack_bitfield = 0;
        uint16_t fragment_index = 0;
        uint16_t fragment_count = 1;
        uint32_t body_size = 0;
        uint32_t checksum = 0;

        [[nodiscard]] uint16_t GetMagic() const noexcept { return NetworkToHost(magic); }
        [[nodiscard]] uint16_t GetMessageId() const noexcept { return NetworkToHost(message_id); }
        [[nodiscard]] uint32_t GetSequenceId() const noexcept { return NetworkToHost(sequence_id); }
        [[nodiscard]] uint32_t GetAckId() const noexcept { return NetworkToHost(ack_id); }
        [[nodiscard]] uint32_t GetAckBitfield() const noexcept { return NetworkToHost(ack_bitfield); }
        [[nodiscard]] uint16_t GetFragmentIndex() const noexcept { return NetworkToHost(fragment_index); }
        [[nodiscard]] uint16_t GetFragmentCount() const noexcept { return NetworkToHost(fragment_count); }
        [[nodiscard]] uint32_t GetBodySize() const noexcept { return NetworkToHost(body_size); }
        [[nodiscard]] uint32_t GetChecksum() const noexcept { return NetworkToHost(checksum); }

        void SetMagic(uint16_t m) noexcept { magic = HostToNetwork(m); }
        void SetMessageId(uint16_t id) noexcept { message_id = HostToNetwork(id); }
        void SetSequenceId(uint32_t seq) noexcept { sequence_id = HostToNetwork(seq); }
        void SetAckId(uint32_t ack) noexcept { ack_id = HostToNetwork(ack); }
        void SetAckBitfield(uint32_t bf) noexcept { ack_bitfield = HostToNetwork(bf); }
        void SetFragmentIndex(uint16_t idx) noexcept { fragment_index = HostToNetwork(idx); }
        void SetFragmentCount(uint16_t cnt) noexcept { fragment_count = HostToNetwork(cnt); }
        void SetBodySize(uint32_t size) noexcept { body_size = HostToNetwork(size); }
        void SetChecksum(uint32_t c) noexcept { checksum = HostToNetwork(c); }

        [[nodiscard]] bool IsValidMagic() const noexcept {
            return GetMagic() == MAGIC_UDP;
        }

        [[nodiscard]] bool IsValidSize() const noexcept {
            return GetBodySize() <= MAX_PACKET_SIZE;
        }
    };
#pragma pack(pop)

    // Complete Packet container supporting both TCP and UDP framing
    class Packet {
    public:
        Packet() = default;

        explicit Packet(uint16_t messageId, uint8_t flags = PacketFlags::None) {
            m_tcpHeader.SetMessageId(messageId);
            m_tcpHeader.flags = flags;
            m_udpHeader.SetMessageId(messageId);
            m_udpHeader.flags = flags;
        }

        template <SerializableEnum E>
        explicit Packet(E messageId, uint8_t flags = PacketFlags::None)
            : Packet(static_cast<uint16_t>(messageId), flags) {}

        [[nodiscard]] uint16_t GetId() const noexcept {
            return m_tcpHeader.GetMessageId();
        }

        template <SerializableEnum E>
        [[nodiscard]] E GetIdAs() const noexcept {
            return static_cast<E>(GetId());
        }

        void SetId(uint16_t id) noexcept {
            m_tcpHeader.SetMessageId(id);
            m_udpHeader.SetMessageId(id);
        }

        template <SerializableEnum E>
        void SetId(E id) noexcept {
            SetId(static_cast<uint16_t>(id));
        }

        [[nodiscard]] uint8_t GetFlags() const noexcept {
            return m_tcpHeader.flags;
        }

        void SetFlags(uint8_t flags) noexcept {
            m_tcpHeader.flags = flags;
            m_udpHeader.flags = flags;
        }

        void AddFlag(uint8_t flag) noexcept {
            m_tcpHeader.flags |= flag;
            m_udpHeader.flags |= flag;
        }

        [[nodiscard]] bool HasFlag(uint8_t flag) const noexcept {
            return (m_tcpHeader.flags & flag) != 0;
        }

        [[nodiscard]] const std::vector<uint8_t>& Body() const noexcept {
            return m_writer.Buffer();
        }

        [[nodiscard]] std::vector<uint8_t>& Body() noexcept {
            return m_writer.Buffer();
        }

        [[nodiscard]] size_t BodySize() const noexcept {
            return m_writer.Size();
        }

        [[nodiscard]] size_t TotalTCPSize() const noexcept {
            return sizeof(TCPHeader) + m_writer.Size();
        }

        [[nodiscard]] size_t TotalUDPSize() const noexcept {
            return sizeof(UDPHeader) + m_writer.Size();
        }

        [[nodiscard]] std::span<const uint8_t> BodySpan() const noexcept {
            return m_writer.Span();
        }

        [[nodiscard]] BufferView View() const noexcept {
            return m_writer.View();
        }

        [[nodiscard]] ByteReader GetReader() const noexcept {
            return ByteReader(m_writer.Span());
        }

        ByteWriter& GetWriter() noexcept {
            return m_writer;
        }

        void SetBody(std::span<const uint8_t> data) {
            m_writer.Clear();
            m_writer.WriteBytes(data);
        }

        void SetBody(std::vector<uint8_t> data) {
            m_writer.Clear();
            m_writer.WriteBytes(data);
        }

        void FinalizeTCP() {
            uint32_t size = static_cast<uint32_t>(m_writer.Size());
            m_tcpHeader.SetBodySize(size);
            m_tcpHeader.SetChecksum(CRC32::Calculate(m_writer.Span()));
        }

        void FinalizeUDP(uint32_t sequenceId = 0, uint32_t ackId = 0, uint32_t ackBitfield = 0) {
            uint32_t size = static_cast<uint32_t>(m_writer.Size());
            m_udpHeader.SetBodySize(size);
            m_udpHeader.SetSequenceId(sequenceId);
            m_udpHeader.SetAckId(ackId);
            m_udpHeader.SetAckBitfield(ackBitfield);
            m_udpHeader.SetChecksum(CRC32::Calculate(m_writer.Span()));
        }

        [[nodiscard]] const TCPHeader& GetTCPHeader() const noexcept { return m_tcpHeader; }
        [[nodiscard]] TCPHeader& GetTCPHeader() noexcept { return m_tcpHeader; }

        [[nodiscard]] const UDPHeader& GetUDPHeader() const noexcept { return m_udpHeader; }
        [[nodiscard]] UDPHeader& GetUDPHeader() noexcept { return m_udpHeader; }

        [[nodiscard]] bool ValidateTCPChecksum() const noexcept {
            return CRC32::Calculate(m_writer.Span()) == m_tcpHeader.GetChecksum();
        }

        [[nodiscard]] bool ValidateUDPChecksum() const noexcept {
            return CRC32::Calculate(m_writer.Span()) == m_udpHeader.GetChecksum();
        }

        // Forward serialization operators
        template <typename T>
        Packet& operator<<(const T& value) {
            m_writer << value;
            return *this;
        }

        void Clear() {
            m_writer.Clear();
            m_tcpHeader = TCPHeader{};
            m_udpHeader = UDPHeader{};
        }

    private:
        TCPHeader m_tcpHeader{};
        UDPHeader m_udpHeader{};
        ByteWriter m_writer;
    };

} // namespace darkfire::net
