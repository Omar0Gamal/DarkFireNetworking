#pragma once

#include "tcp_connection.hpp"
#include "lockfree_queue.hpp"
#include "log.hpp"

#include <memory>
#include <string>
#include <thread>
#include <future>

namespace darkfire::net {

    template <typename MessageType = uint16_t>
    class TCPClient {
    public:
        explicit TCPClient(std::string authKey = "DarkFireSecretAuthKey")
            : m_authKey(std::move(authKey)) {}

        virtual ~TCPClient() {
            Disconnect();
        }

        bool Connect(const std::string& host, uint16_t port) {
            try {
                asio::ip::tcp::resolver resolver(m_ioContext);
                auto endpoints = resolver.resolve(host, std::to_string(port));

                m_connection = std::make_shared<TCPConnection>(
                    TCPConnection::Owner::Client,
                    m_ioContext,
                    asio::ip::tcp::socket(m_ioContext),
                    0,
                    m_authKey
                );

                m_connection->SetMessageCallback([this](std::shared_ptr<TCPConnection> conn, Packet& pkt) {
                    if (m_asyncDispatch.load()) {
                        OnMessage(pkt);
                    } else {
                        m_qIncoming.Push({ conn, pkt });
                    }
                });

                m_connection->SetDisconnectCallback([this](std::shared_ptr<TCPConnection>) {
                    OnDisconnected();
                });

                m_connection->SetValidatedCallback([this](std::shared_ptr<TCPConnection>) {
                    OnConnected();
                });

                // Spawn connection coroutine
                std::promise<bool> connectPromise;
                auto connectFuture = connectPromise.get_future();

                asio::co_spawn(m_ioContext, 
                    [this, endpoints, &connectPromise]() -> asio::awaitable<void> {
                        bool ok = co_await m_connection->ConnectClient(endpoints);
                        connectPromise.set_value(ok);
                    },
                    asio::detached
                );

                m_workerThread = std::thread([this]() {
                    auto work = asio::make_work_guard(m_ioContext);
                    m_ioContext.run();
                });

                // Wait up to 1.5 seconds for initial socket connection
                if (connectFuture.wait_for(std::chrono::milliseconds(1500)) == std::future_status::ready) {
                    return connectFuture.get();
                }

                LogError("Connection timed out to ", host, ":", port);
                Disconnect();
                return false;
            } catch (const std::exception& e) {
                LogError("Client exception: ", e.what());
                return false;
            }
        }

        void Disconnect() {
            if (m_connection) {
                m_connection->Disconnect();
            }

            m_ioContext.stop();

            if (m_workerThread.joinable()) {
                m_workerThread.join();
            }

            m_ioContext.restart();
            m_connection.reset();
        }

        [[nodiscard]] bool IsConnected() const noexcept {
            return m_connection && m_connection->IsActive();
        }

        void Send(const Packet& packet) {
            if (IsConnected()) {
                m_connection->Send(packet);
            }
        }

        ThreadSafeQueue<OwnedPacket>& Incoming() noexcept {
            return m_qIncoming;
        }

        // Ticks incoming messages from the calling thread (e.g. main/render loop)
        void Update(size_t maxMessages = SIZE_MAX) {
            size_t count = 0;
            OwnedPacket item;
            while (count < maxMessages && m_qIncoming.Pop(item)) {
                OnMessage(item.packet);
                count++;
            }
        }
        
        void SetAsyncDispatch(bool enable = true) noexcept {
            m_asyncDispatch = enable;
        }

    protected:
        virtual void OnConnected() {}
        virtual void OnDisconnected() {}
        virtual void OnMessage(Packet&) {}

    protected:
        asio::io_context m_ioContext;
        std::thread m_workerThread;
        std::atomic<bool> m_asyncDispatch{ false };
        std::shared_ptr<TCPConnection> m_connection;
        ThreadSafeQueue<OwnedPacket> m_qIncoming;
        std::string m_authKey;
    };

} // namespace darkfire::net
