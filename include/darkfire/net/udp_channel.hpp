#pragma once

#ifndef ASIO_STANDALONE
#define ASIO_STANDALONE
#endif

#ifdef _WIN32
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0A00
#endif
#endif

#include <asio.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/as_tuple.hpp>

#include "error.hpp"
#include "packet.hpp"
#include "log.hpp"
#include "lockfree_queue.hpp"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>

namespace darkfire::net {

    // Delivery modes for UDP channels
    enum class UDPDeliveryMode {
        Unreliable,
        UnreliableSequenced,
        Reliable
    };

    // Helper for sequence number wrap-around comparison (delegates to RFC 1982 implementation)
    inline bool SequenceGreaterThan(uint32_t s1, uint32_t s2) noexcept {
        return sequence_greater_than<uint32_t>(s1, s2);
    }

    struct UDPOwnedPacket {
        asio::ip::udp::endpoint endpoint;
        Packet packet;
    };

    using UDPMessageCallback = std::function<void(const asio::ip::udp::endpoint&, Packet&)>;

    // State tracked per remote UDP peer for sequencing, reliability, and fragment reassembly
    struct UDPPeerState {
        uint32_t localSequence = 0;
        uint32_t remoteSequence = 0;
        uint32_t ackBitfield = 0;
        bool hasReceivedPackets = false;

        // Sequence filter tracking (RFC 1982)
        bool sequenceFilterEnabled = false;
        bool hasReceivedSequenced = false;
        uint32_t highestSequencedId = 0;

        // Pending unacknowledged reliable packets
        struct PendingPacket {
            uint32_t sequence = 0;
            Packet packet;
            std::chrono::steady_clock::time_point sendTime;
            int retransmissions = 0;
        };
        std::vector<PendingPacket> pendingReliable;

        // Fragmentation reassembly buffer
        struct ReassemblyContext {
            uint16_t fragmentCount = 0;
            uint16_t receivedCount = 0;
            std::vector<std::vector<uint8_t>> parts;
            std::chrono::steady_clock::time_point timestamp;
        };
        // Key: sequence_id
        std::unordered_map<uint32_t, ReassemblyContext> reassemblyMap;
    };

    class UDPChannel : public std::enable_shared_from_this<UDPChannel> {
    public:
        UDPChannel(asio::io_context& ioContext, uint16_t bindPort = 0)
            : m_ioContext(ioContext),
              m_socket(ioContext),
              m_maintenanceTimer(ioContext)
        {
            asio::ip::udp::endpoint ep(asio::ip::udp::v4(), bindPort);
            m_socket.open(ep.protocol());
            m_socket.set_option(asio::socket_base::reuse_address(true));
            m_socket.bind(ep);
        }

        ~UDPChannel() {
            Close();
        }

        void Start() {
            m_running = true;

            // Spawn Receiver coroutine
            asio::co_spawn(m_ioContext, 
                [this, self = shared_from_this()]() -> asio::awaitable<void> {
                    co_await ReceiverWorker();
                },
                asio::detached
            );

            // Spawn Maintenance coroutine (retransmissions, timeouts)
            asio::co_spawn(m_ioContext,
                [this, self = shared_from_this()]() -> asio::awaitable<void> {
                    co_await MaintenanceWorker();
                },
                asio::detached
            );
        }

        void Close() {
            if (!m_running) return;
            m_running = false;

            std::error_code ec;
            m_socket.close(ec);
            m_maintenanceTimer.cancel(ec);
        }

        void SetMessageCallback(UDPMessageCallback cb) {
            m_onMessage = std::move(cb);
        }

        [[nodiscard]] asio::ip::udp::endpoint LocalEndpoint() const noexcept {
            std::error_code ec;
            return m_socket.local_endpoint(ec);
        }

        // Send unsequenced / fast raw packet
        void SendTo(const asio::ip::udp::endpoint& target, Packet packet) {
            SendInternal(target, std::move(packet), false);
        }

        // Send sequenced packet (discards stale packets at receiver)
        void SendSequenced(const asio::ip::udp::endpoint& target, Packet packet) {
            packet.AddFlag(PacketFlags::Sequenced);
            SendInternal(target, std::move(packet), false);
        }

        // Send reliable UDP packet (with selective ack and retransmissions)
        void SendReliable(const asio::ip::udp::endpoint& target, Packet packet) {
            packet.AddFlag(PacketFlags::Reliable);
            SendInternal(target, std::move(packet), true);
        }

        // Enable or disable sequence filtering channel-wide for all endpoints
        void SetUnreliableSequencedMode(bool enabled) noexcept {
            m_sequenceFilterAll = enabled;
        }

        void SetSequenceFilterEnabled(bool enabled) noexcept {
            m_sequenceFilterAll = enabled;
        }

        [[nodiscard]] bool IsSequenceFilterEnabled() const noexcept {
            return m_sequenceFilterAll;
        }

        // Enable or disable sequence filtering for a specific remote endpoint
        void SetEndpointSequencedMode(const asio::ip::udp::endpoint& ep, bool enabled) {
            std::lock_guard<std::mutex> lock(m_peerMutex);
            m_peers[ep].sequenceFilterEnabled = enabled;
        }

        void SetEndpointSequenceFilter(const asio::ip::udp::endpoint& ep, bool enabled) {
            SetEndpointSequencedMode(ep, enabled);
        }

        // Helper to evaluate/filter incoming sequence numbers under RFC 1982 rules
        bool FilterIncomingSequence(const asio::ip::udp::endpoint& senderEp, uint32_t packetSeq, uint8_t flags = PacketFlags::Sequenced) {
            std::lock_guard<std::mutex> lock(m_peerMutex);
            auto& peer = m_peers[senderEp];
            bool isSequenced = (flags & PacketFlags::Sequenced) || m_sequenceFilterAll || peer.sequenceFilterEnabled;
            if (!isSequenced) {
                return true;
            }
            if (!peer.hasReceivedSequenced) {
                peer.hasReceivedSequenced = true;
                peer.highestSequencedId = packetSeq;
                return true;
            }
            if (sequence_greater_than(packetSeq, peer.highestSequencedId)) {
                peer.highestSequencedId = packetSeq;
                return true;
            }
            return false;
        }

        [[nodiscard]] uint32_t GetHighestReceivedSequence(const asio::ip::udp::endpoint& ep) const {
            std::lock_guard<std::mutex> lock(m_peerMutex);
            auto it = m_peers.find(ep);
            if (it != m_peers.end()) {
                return it->second.highestSequencedId;
            }
            return 0;
        }

    private:
        void SendInternal(const asio::ip::udp::endpoint& target, Packet packet, bool reliable) {
            if (!m_running) return;

            std::lock_guard<std::mutex> lock(m_peerMutex);
            auto& peer = m_peers[target];
            uint32_t seq = ++peer.localSequence;

            // Check if fragmentation is required (payload > STANDARD_MTU)
            if (packet.BodySize() > STANDARD_MTU) {
                SendFragmented(target, packet, seq, peer.remoteSequence, peer.ackBitfield, reliable);
                return;
            }

            packet.FinalizeUDP(seq, peer.remoteSequence, peer.ackBitfield);

            // Construct buffer: UDPHeader + Body
            std::vector<uint8_t> buffer(sizeof(UDPHeader) + packet.BodySize());
            std::memcpy(buffer.data(), &packet.GetUDPHeader(), sizeof(UDPHeader));
            if (packet.BodySize() > 0) {
                std::memcpy(buffer.data() + sizeof(UDPHeader), packet.Body().data(), packet.BodySize());
            }

            if (reliable) {
                peer.pendingReliable.push_back({
                    seq,
                    packet,
                    std::chrono::steady_clock::now(),
                    0
                });
            }

            // Post async send with buffer lifetime extension
            auto sendBuf = std::make_shared<std::vector<uint8_t>>(std::move(buffer));
            m_socket.async_send_to(
                asio::buffer(*sendBuf), target,
                [sendBuf, target](std::error_code ec, size_t) {
                    if (ec && ec != asio::error::operation_aborted) {
                        LogError("UDP send error to ", target.address().to_string(), ": ", ec.message());
                    }
                }
            );
        }

        void SendFragmented(
            const asio::ip::udp::endpoint& target,
            const Packet& packet,
            uint32_t seq,
            uint32_t ackId,
            uint32_t ackBitfield,
            bool reliable
        ) {
            const auto& body = packet.Body();
            size_t totalBytes = body.size();
            uint16_t totalFrags = static_cast<uint16_t>((totalBytes + STANDARD_MTU - 1) / STANDARD_MTU);

            for (uint16_t i = 0; i < totalFrags; ++i) {
                size_t offset = i * STANDARD_MTU;
                size_t chunkSize = std::min<size_t>(STANDARD_MTU, totalBytes - offset);

                Packet fragPacket(packet.GetId(), packet.GetFlags() | PacketFlags::Fragmented);
                if (reliable) fragPacket.AddFlag(PacketFlags::Reliable);

                fragPacket.GetWriter().WriteBytes(body.data() + offset, chunkSize);
                auto& hdr = fragPacket.GetUDPHeader();
                hdr.SetFragmentIndex(i);
                hdr.SetFragmentCount(totalFrags);
                fragPacket.FinalizeUDP(seq, ackId, ackBitfield);

                std::vector<uint8_t> buffer(sizeof(UDPHeader) + fragPacket.BodySize());
                std::memcpy(buffer.data(), &fragPacket.GetUDPHeader(), sizeof(UDPHeader));
                std::memcpy(buffer.data() + sizeof(UDPHeader), fragPacket.Body().data(), fragPacket.BodySize());

                auto fragBuf = std::make_shared<std::vector<uint8_t>>(std::move(buffer));
                m_socket.async_send_to(
                    asio::buffer(*fragBuf), target,
                    [fragBuf](std::error_code, size_t) {}
                );
            }
        }

        asio::awaitable<void> ReceiverWorker() {
            std::vector<uint8_t> recvBuf(MAX_PACKET_SIZE + sizeof(UDPHeader));

            while (m_running) {
                asio::ip::udp::endpoint senderEp;
                auto [ec, bytesReceived] = co_await m_socket.async_receive_from(
                    asio::buffer(recvBuf), senderEp,
                    asio::as_tuple(asio::use_awaitable)
                );

                if (ec) {
                    if (ec != asio::error::operation_aborted) {
                        LogWarn("UDP receive error: ", ec.message());
                    }
                    break;
                }

                if (bytesReceived < sizeof(UDPHeader)) {
                    continue; // Runt packet
                }

                UDPHeader header{};
                std::memcpy(&header, recvBuf.data(), sizeof(UDPHeader));

                if (!header.IsValidMagic()) {
                    continue; // Ignore alien datagrams
                }

                if (!header.IsValidSize() || bytesReceived < sizeof(UDPHeader) + header.GetBodySize()) {
                    continue; // Incomplete or oversized
                }

                // Checksum validation
                uint32_t expectedChecksum = header.GetChecksum();
                uint32_t computedChecksum = CRC32::Calculate(
                    recvBuf.data() + sizeof(UDPHeader),
                    header.GetBodySize()
                );

                if (computedChecksum != expectedChecksum) {
                    LogWarn("UDP packet checksum mismatch from ", senderEp.address().to_string());
                    continue;
                }

                ProcessIncomingPacket(senderEp, header, recvBuf.data() + sizeof(UDPHeader));
            }
        }

        void ProcessIncomingPacket(
            const asio::ip::udp::endpoint& senderEp,
            const UDPHeader& header,
            const uint8_t* bodyData
        ) {
            uint32_t packetSeq = header.GetSequenceId();
            uint32_t ackId = header.GetAckId();
            uint32_t ackBitfield = header.GetAckBitfield();

            std::lock_guard<std::mutex> lock(m_peerMutex);
            auto& peer = m_peers[senderEp];

            // 1. Process ACKs from remote peer for our pending reliable packets
            if (!peer.pendingReliable.empty()) {
                auto it = peer.pendingReliable.begin();
                while (it != peer.pendingReliable.end()) {
                    bool acked = false;
                    if (it->sequence == ackId) {
                        acked = true;
                    } else if (SequenceGreaterThan(ackId, it->sequence)) {
                        uint32_t diff = ackId - it->sequence;
                        if (diff <= 32 && (ackBitfield & (1u << (diff - 1)))) {
                            acked = true;
                        }
                    }

                    if (acked) {
                        it = peer.pendingReliable.erase(it);
                    } else {
                        ++it;
                    }
                }
            }

            // 2. Sequencing & Ack update
            bool isSequenced = (header.flags & PacketFlags::Sequenced) || m_sequenceFilterAll || peer.sequenceFilterEnabled;
            if (isSequenced && !(header.flags & PacketFlags::Reliable)) {
                if (peer.hasReceivedSequenced) {
                    if (!sequence_greater_than(packetSeq, peer.highestSequencedId)) {
                        return; // Dropped stale or duplicate sequenced packet
                    }
                }
                peer.hasReceivedSequenced = true;
                peer.highestSequencedId = packetSeq;
            }

            if (!peer.hasReceivedPackets) {
                peer.hasReceivedPackets = true;
                peer.remoteSequence = packetSeq;
                peer.ackBitfield = 0;
            } else if (SequenceGreaterThan(packetSeq, peer.remoteSequence)) {
                uint32_t diff = packetSeq - peer.remoteSequence;
                if (diff <= 32) {
                    peer.ackBitfield = (peer.ackBitfield << diff) | (1u << (diff - 1));
                } else {
                    peer.ackBitfield = 0;
                }
                peer.remoteSequence = packetSeq;
            } else {
                // Out-of-order or duplicate packet
                uint32_t diff = peer.remoteSequence - packetSeq;
                if (diff > 0 && diff <= 32) {
                    peer.ackBitfield |= (1u << (diff - 1));
                }
                // If it's not marked reliable and sequenced filtering is active, discard
                if (!(header.flags & PacketFlags::Reliable) && isSequenced) {
                    return; // Dropped out-of-order unreliable packet
                }
            }

            // 3. Handle Fragmentation / Reassembly
            if (header.flags & PacketFlags::Fragmented) {
                auto& reassembly = peer.reassemblyMap[packetSeq];
                if (reassembly.parts.empty()) {
                    reassembly.fragmentCount = header.GetFragmentCount();
                    reassembly.receivedCount = 0;
                    reassembly.parts.resize(reassembly.fragmentCount);
                    reassembly.timestamp = std::chrono::steady_clock::now();
                }

                uint16_t fragIdx = header.GetFragmentIndex();
                if (fragIdx < reassembly.fragmentCount && reassembly.parts[fragIdx].empty()) {
                    reassembly.parts[fragIdx].assign(bodyData, bodyData + header.GetBodySize());
                    reassembly.receivedCount++;
                }

                if (reassembly.receivedCount == reassembly.fragmentCount) {
                    // All fragments received: assemble into full packet
                    Packet fullPacket(header.GetMessageId(), header.flags & ~PacketFlags::Fragmented);
                    for (const auto& part : reassembly.parts) {
                        fullPacket.GetWriter().WriteBytes(part);
                    }
                    peer.reassemblyMap.erase(packetSeq);

                    if (m_onMessage) {
                        m_onMessage(senderEp, fullPacket);
                    }
                }
                return;
            }

            // Normal unfragmented packet
            Packet packet(header.GetMessageId(), header.flags);
            packet.GetUDPHeader() = header;
            packet.SetBody(std::span<const uint8_t>(bodyData, header.GetBodySize()));

            if (m_onMessage) {
                m_onMessage(senderEp, packet);
            }
        }

        asio::awaitable<void> MaintenanceWorker() {
            while (m_running) {
                m_maintenanceTimer.expires_after(std::chrono::milliseconds(50));
                auto [ec] = co_await m_maintenanceTimer.async_wait(asio::as_tuple(asio::use_awaitable));
                if (ec) break;

                auto now = std::chrono::steady_clock::now();
                std::lock_guard<std::mutex> lock(m_peerMutex);

                for (auto& [ep, peer] : m_peers) {
                    // Check pending reliable packets for retransmission (timeout = 150ms)
                    for (auto& pending : peer.pendingReliable) {
                        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - pending.sendTime).count();
                        if (elapsed > 150) {
                            pending.sendTime = now;
                            pending.retransmissions++;

                            // Retransmit
                            pending.packet.FinalizeUDP(pending.sequence, peer.remoteSequence, peer.ackBitfield);
                            std::vector<uint8_t> buffer(sizeof(UDPHeader) + pending.packet.BodySize());
                            std::memcpy(buffer.data(), &pending.packet.GetUDPHeader(), sizeof(UDPHeader));
                            if (pending.packet.BodySize() > 0) {
                                std::memcpy(buffer.data() + sizeof(UDPHeader), pending.packet.Body().data(), pending.packet.BodySize());
                            }

                            auto retxBuf = std::make_shared<std::vector<uint8_t>>(std::move(buffer));
                            m_socket.async_send_to(asio::buffer(*retxBuf), ep, [retxBuf](std::error_code, size_t) {});
                        }
                    }

                    // Clean up timed out reassembly entries (> 3 seconds)
                    auto it = peer.reassemblyMap.begin();
                    while (it != peer.reassemblyMap.end()) {
                        auto age = std::chrono::duration_cast<std::chrono::seconds>(now - it->second.timestamp).count();
                        if (age > 3) {
                            it = peer.reassemblyMap.erase(it);
                        } else {
                            ++it;
                        }
                    }
                }
            }
        }

    private:
        asio::io_context& m_ioContext;
        asio::ip::udp::socket m_socket;
        asio::steady_timer m_maintenanceTimer;
        std::atomic<bool> m_running{ false };
        std::atomic<bool> m_sequenceFilterAll{ false };

        mutable std::mutex m_peerMutex;
        // Hash for udp::endpoint
        struct EndpointHash {
            size_t operator()(const asio::ip::udp::endpoint& ep) const noexcept {
                return std::hash<std::string>()(ep.address().to_string()) ^ (std::hash<uint16_t>()(ep.port()) << 1);
            }
        };
        std::unordered_map<asio::ip::udp::endpoint, UDPPeerState, EndpointHash> m_peers;
        UDPMessageCallback m_onMessage;
    };

} // namespace darkfire::net
