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
#include <asio/redirect_error.hpp>
#include <asio/cancellation_signal.hpp>

#include "error.hpp"
#include "log.hpp"
#include "packet.hpp"
#include "security.hpp"
#include "lockfree_queue.hpp"

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <functional>

namespace darkfire::net {

    enum class SessionState {
        Connected,
        Authenticating,
        Active,
        Disconnecting,
        Disconnected
    };

    inline const char* SessionStateToString(SessionState state) noexcept {
        switch (state) {
            case SessionState::Connected:      return "Connected";
            case SessionState::Authenticating: return "Authenticating";
            case SessionState::Active:         return "Active";
            case SessionState::Disconnecting:  return "Disconnecting";
            case SessionState::Disconnected:   return "Disconnected";
            default:                           return "Unknown";
        }
    }

    class TCPConnection;

    struct OwnedPacket {
        std::shared_ptr<TCPConnection> connection;
        Packet packet;
    };

    using MessageCallback = std::function<void(std::shared_ptr<TCPConnection>, Packet&)>;
    using DisconnectCallback = std::function<void(std::shared_ptr<TCPConnection>)>;
    using ValidatedCallback = std::function<void(std::shared_ptr<TCPConnection>)>;

    class TCPConnection : public std::enable_shared_from_this<TCPConnection> {
    public:
        enum class Owner { Server, Client };

        TCPConnection(
            Owner owner,
            asio::io_context& ioContext,
            asio::ip::tcp::socket socket,
            uint32_t id = 0,
            std::string authKey = "DarkFireSecretAuthKey"
        ) : m_owner(owner),
            m_ioContext(ioContext),
            m_socket(std::move(socket)),
            m_id(id),
            m_authKey(std::move(authKey)),
            m_writeTimer(ioContext)
        {
            // Socket optimizations
            std::error_code ec;
            m_socket.set_option(asio::ip::tcp::no_delay(true), ec);
            m_socket.set_option(asio::socket_base::keep_alive(true), ec);
        }

        ~TCPConnection() {
            Disconnect();
        }

        [[nodiscard]] uint32_t GetId() const noexcept { return m_id; }
        void SetId(uint32_t id) noexcept { m_id = id; }

        [[nodiscard]] SessionState GetState() const noexcept { return m_state.load(); }
        [[nodiscard]] bool IsConnected() const noexcept { 
            return m_state.load() == SessionState::Active || m_state.load() == SessionState::Authenticating; 
        }
        [[nodiscard]] bool IsActive() const noexcept { return m_state.load() == SessionState::Active; }

        [[nodiscard]] asio::ip::tcp::endpoint GetRemoteEndpoint() const noexcept {
            std::error_code ec;
            return m_socket.remote_endpoint(ec);
        }

        void SetMessageCallback(MessageCallback cb) { m_onMessage = std::move(cb); }
        void SetDisconnectCallback(DisconnectCallback cb) { m_onDisconnect = std::move(cb); }
        void SetValidatedCallback(ValidatedCallback cb) { m_onValidated = std::move(cb); }

        void StartServerSession() {
            m_state.store(SessionState::Connected);
            auto self = shared_from_this();
            asio::co_spawn(m_ioContext, 
                asio::bind_cancellation_slot(m_cancelSignal.slot(), [this, self]() -> asio::awaitable<void> {
                    co_await ServerSessionWorker();
                }),
                asio::detached
            );
        }

        asio::awaitable<bool> ConnectClient(const asio::ip::tcp::resolver::results_type& endpoints) {
            m_state.store(SessionState::Connected);
            std::error_code ec;
            auto [connectEc, ep] = co_await asio::async_connect(
                m_socket, endpoints, 
                asio::as_tuple(asio::use_awaitable)
            );

            if (connectEc) {
                LogWarn("Client failed to connect: ", connectEc.message());
                m_state.store(SessionState::Disconnected);
                co_return false;
            }

            LogInfo("Client connected to ", ep.address().to_string(), ":", ep.port());

            auto self = shared_from_this();
            asio::co_spawn(m_ioContext,
                asio::bind_cancellation_slot(m_cancelSignal.slot(), [this, self]() -> asio::awaitable<void> {
                    co_await ClientSessionWorker();
                }),
                asio::detached
            );

            co_return true;
        }

        void Send(Packet packet) {
            if (!IsConnected()) {
                return;
            }
            packet.FinalizeTCP();
            m_outQueue.Push(std::move(packet));
            
            // Wake up writer worker
            asio::post(m_ioContext, [this, self = shared_from_this()]() {
                m_writeTimer.cancel_one();
            });
        }

        void Disconnect() {
            SessionState expected = m_state.load();
            if (expected == SessionState::Disconnected || expected == SessionState::Disconnecting) {
                return;
            }

            m_state.store(SessionState::Disconnecting);
            m_cancelSignal.emit(asio::cancellation_type::all);

            asio::post(m_ioContext, [this, self = shared_from_this()]() {
                std::error_code ec;
                if (m_socket.is_open()) {
                    m_socket.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
                    m_socket.close(ec);
                }
                m_writeTimer.cancel(ec);
                m_state.store(SessionState::Disconnected);

                if (m_onDisconnect) {
                    m_onDisconnect(self);
                }
            });
        }

    private:
        asio::awaitable<void> ServerSessionWorker() {
            // Handshake Phase
            m_state.store(SessionState::Authenticating);
            uint64_t nonce = SecurityUtils::GenerateSecureNonce();

            // Send Handshake Challenge
            Packet challenge(0, PacketFlags::HandshakeChallenge);
            challenge << nonce;
            challenge.FinalizeTCP();

            std::array<asio::const_buffer, 2> writeBufs = {
                asio::buffer(&challenge.GetTCPHeader(), sizeof(TCPHeader)),
                asio::buffer(challenge.Body().data(), challenge.Body().size())
            };

            auto [wEc, wBytes] = co_await asio::async_write(m_socket, writeBufs, asio::as_tuple(asio::use_awaitable));
            if (wEc) {
                LogError("[Server] Failed to write handshake challenge: ", wEc.message());
                Disconnect();
                co_return;
            }

            // Expect Handshake Response
            TCPHeader respHeader{};
            auto [rEc, rBytes] = co_await asio::async_read(m_socket, asio::buffer(&respHeader, sizeof(respHeader)), asio::as_tuple(asio::use_awaitable));
            if (rEc || !respHeader.IsValidMagic() || !(respHeader.flags & PacketFlags::HandshakeResponse)) {
                LogError("[Server] Invalid handshake response header from client [", m_id, "]");
                Disconnect();
                co_return;
            }

            std::vector<uint8_t> respBody(respHeader.GetBodySize());
            if (respBody.size() != SHA256::DigestSize) {
                LogError("[Server] Invalid handshake digest length from client [", m_id, "]");
                Disconnect();
                co_return;
            }

            auto [bEc, bBytes] = co_await asio::async_read(m_socket, asio::buffer(respBody.data(), respBody.size()), asio::as_tuple(asio::use_awaitable));
            if (bEc) {
                LogError("[Server] Failed reading handshake response body: ", bEc.message());
                Disconnect();
                co_return;
            }

            // Verify HMAC-SHA256 over the wire challenge bytes
            auto expectedHmac = HMAC_SHA256::Compute(
                std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(m_authKey.data()), m_authKey.size()),
                std::span<const uint8_t>(challenge.Body().data(), challenge.Body().size())
            );

            if (!SecurityUtils::ConstantTimeCompare(respBody, expectedHmac)) {
                LogError("[Server] Client [", m_id, "] failed HMAC authentication challenge!");
                Disconnect();
                co_return;
            }

            LogInfo("[Server] Client [", m_id, "] authenticated successfully");
            m_state.store(SessionState::Active);

            if (m_onValidated) {
                m_onValidated(shared_from_this());
            }

            // Spawn concurrent Reader and Writer coroutines
            auto self = shared_from_this();
            asio::co_spawn(m_ioContext, 
                asio::bind_cancellation_slot(m_cancelSignal.slot(), [this, self]() -> asio::awaitable<void> {
                    co_await WriterWorker();
                }),
                asio::detached
            );

            co_await ReaderWorker();
        }

        asio::awaitable<void> ClientSessionWorker() {
            // Handshake Phase
            m_state.store(SessionState::Authenticating);

            // Read Challenge
            TCPHeader challengeHeader{};
            auto [rEc, rBytes] = co_await asio::async_read(m_socket, asio::buffer(&challengeHeader, sizeof(challengeHeader)), asio::as_tuple(asio::use_awaitable));
            if (rEc || !challengeHeader.IsValidMagic() || !(challengeHeader.flags & PacketFlags::HandshakeChallenge)) {
                LogError("[Client] Invalid handshake challenge from server");
                Disconnect();
                co_return;
            }

            std::vector<uint8_t> challengeBody(challengeHeader.GetBodySize());
            auto [bEc, bBytes] = co_await asio::async_read(m_socket, asio::buffer(challengeBody.data(), challengeBody.size()), asio::as_tuple(asio::use_awaitable));
            if (bEc || challengeBody.size() != sizeof(uint64_t)) {
                LogError("[Client] Failed reading handshake challenge body");
                Disconnect();
                co_return;
            }

            // Compute HMAC over wire challenge bytes
            auto hmac = HMAC_SHA256::Compute(
                std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(m_authKey.data()), m_authKey.size()),
                std::span<const uint8_t>(challengeBody.data(), challengeBody.size())
            );

            // Send Response
            Packet response(0, PacketFlags::HandshakeResponse);
            response.GetWriter().WriteBytes(hmac);
            response.FinalizeTCP();

            std::array<asio::const_buffer, 2> writeBufs = {
                asio::buffer(&response.GetTCPHeader(), sizeof(TCPHeader)),
                asio::buffer(response.Body().data(), response.Body().size())
            };

            auto [wEc, wBytes] = co_await asio::async_write(m_socket, writeBufs, asio::as_tuple(asio::use_awaitable));
            if (wEc) {
                LogError("[Client] Failed writing handshake response: ", wEc.message());
                Disconnect();
                co_return;
            }

            LogInfo("[Client] Handshake completed successfully, session active");
            m_state.store(SessionState::Active);

            if (m_onValidated) {
                m_onValidated(shared_from_this());
            }

            // Spawn concurrent Writer coroutine
            auto self = shared_from_this();
            asio::co_spawn(m_ioContext,
                asio::bind_cancellation_slot(m_cancelSignal.slot(), [this, self]() -> asio::awaitable<void> {
                    co_await WriterWorker();
                }),
                asio::detached
            );

            co_await ReaderWorker();
        }

        asio::awaitable<void> ReaderWorker() {
            auto self = shared_from_this();
            while (m_state.load() == SessionState::Active) {
                TCPHeader header{};
                auto [hEc, hBytes] = co_await asio::async_read(
                    m_socket, asio::buffer(&header, sizeof(header)), 
                    asio::as_tuple(asio::use_awaitable)
                );

                if (hEc) {
                    if (hEc != asio::error::operation_aborted) {
                        LogDebug("Reader disconnected: ", hEc.message());
                    }
                    break;
                }

                if (!header.IsValidMagic()) {
                    LogError("Corrupted packet received: invalid magic 0x", std::hex, header.GetMagic());
                    break;
                }

                if (!header.IsValidSize()) {
                    LogError("Packet exceeds max size: ", header.GetBodySize());
                    break;
                }

                Packet packet;
                packet.GetTCPHeader() = header;

                if (header.GetBodySize() > 0) {
                    packet.Body().resize(header.GetBodySize());
                    auto [bEc, bBytes] = co_await asio::async_read(
                        m_socket, asio::buffer(packet.Body().data(), packet.Body().size()),
                        asio::as_tuple(asio::use_awaitable)
                    );

                    if (bEc) {
                        LogDebug("Error reading packet body: ", bEc.message());
                        break;
                    }
                }

                if (!packet.ValidateTCPChecksum()) {
                    LogError("Packet checksum validation failed for MsgID: ", header.GetMessageId());
                    continue; // Skip corrupted packet
                }

                if (m_onMessage) {
                    m_onMessage(self, packet);
                }
            }

            Disconnect();
        }

        asio::awaitable<void> WriterWorker() {
            auto self = shared_from_this();
            while (m_state.load() == SessionState::Active || m_state.load() == SessionState::Authenticating) {
                Packet pkt;
                bool hadData = false;

                while (m_outQueue.Pop(pkt)) {
                    hadData = true;
                    std::array<asio::const_buffer, 2> bufs = {
                        asio::buffer(&pkt.GetTCPHeader(), sizeof(TCPHeader)),
                        asio::buffer(pkt.Body().data(), pkt.Body().size())
                    };

                    auto [wEc, wBytes] = co_await asio::async_write(
                        m_socket, bufs, 
                        asio::as_tuple(asio::use_awaitable)
                    );

                    if (wEc) {
                        if (wEc != asio::error::operation_aborted) {
                            LogError("Write error: ", wEc.message());
                        }
                        Disconnect();
                        co_return;
                    }
                }

                // Sleep until new packet arrives or session cancelled
                std::error_code tEc;
                m_writeTimer.expires_at((std::chrono::steady_clock::time_point::max)());
                auto [waitEc] = co_await m_writeTimer.async_wait(asio::as_tuple(asio::use_awaitable));
                
                if (m_state.load() != SessionState::Active && m_state.load() != SessionState::Authenticating) {
                    break;
                }
            }
        }

    private:
        Owner m_owner;
        asio::io_context& m_ioContext;
        asio::ip::tcp::socket m_socket;
        uint32_t m_id = 0;
        std::string m_authKey;

        std::atomic<SessionState> m_state{ SessionState::Disconnected };
        asio::cancellation_signal m_cancelSignal;
        asio::steady_timer m_writeTimer;

        ThreadSafeQueue<Packet> m_outQueue;
        MessageCallback m_onMessage;
        DisconnectCallback m_onDisconnect;
        ValidatedCallback m_onValidated;
    };

} // namespace darkfire::net
