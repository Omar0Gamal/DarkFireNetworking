#pragma once

#include "tcp_client.hpp"
#include "udp_channel.hpp"

#include <string>
#include <memory>

namespace darkfire::net {

    template <typename MessageType = uint16_t>
    class DualClient : public TCPClient<MessageType> {
    public:
        explicit DualClient(std::string authKey = "DarkFireSecretAuthKey")
            : TCPClient<MessageType>(std::move(authKey)) {}

        virtual ~DualClient() {
            DisconnectDual();
        }

        bool ConnectDual(const std::string& host, uint16_t tcpPort, uint16_t udpPort) {
            // 1. Connect TCP
            if (!this->Connect(host, tcpPort)) {
                return false;
            }

            // 2. Resolve UDP endpoint & Initialize UDP Channel
            try {
                asio::ip::udp::resolver resolver(this->m_ioContext);
                auto udpEndpoints = resolver.resolve(host, std::to_string(udpPort));
                if (udpEndpoints.empty()) {
                    this->Disconnect();
                    return false;
                }
                m_serverUdpEndpoint = *udpEndpoints.begin();

                m_udpChannel = std::make_shared<UDPChannel>(this->m_ioContext, static_cast<uint16_t>(0));
                m_udpChannel->SetMessageCallback([this](const asio::ip::udp::endpoint&, Packet& pkt) {
                    m_qUdpIncoming.Push(std::move(pkt));
                });
                m_udpChannel->Start();

                // Send registration packet via UDP
                Packet regPacket(0, PacketFlags::SystemMessage);
                regPacket << this->m_connection->GetId();
                for (int i = 0; i < 3; ++i) {
                    m_udpChannel->SendTo(m_serverUdpEndpoint, regPacket);
                }

                return true;
            } catch (const std::exception& e) {
                LogError("UDP init failed: ", e.what());
                this->Disconnect();
                return false;
            }
        }

        void DisconnectDual() {
            if (m_udpChannel) {
                m_udpChannel->Close();
                m_udpChannel.reset();
            }
            this->Disconnect();
        }

        void RegisterUDP(uint32_t clientId) {
            if (this->m_connection) {
                this->m_connection->SetId(clientId);
            }
            if (m_udpChannel && m_serverUdpEndpoint.port() != 0) {
                Packet regPacket(0, PacketFlags::SystemMessage);
                regPacket << clientId;
                for (int i = 0; i < 3; ++i) {
                    m_udpChannel->SendTo(m_serverUdpEndpoint, regPacket);
                }
            }
        }

        void SendTCP(const Packet& packet) {
            this->Send(packet);
        }

        void SendUDP(const Packet& packet) {
            if (m_udpChannel) {
                m_udpChannel->SendTo(m_serverUdpEndpoint, packet);
            }
        }

        void SendUDPSequenced(const Packet& packet) {
            if (m_udpChannel) {
                m_udpChannel->SendSequenced(m_serverUdpEndpoint, packet);
            }
        }

        void SendUDPReliable(const Packet& packet) {
            if (m_udpChannel) {
                m_udpChannel->SendReliable(m_serverUdpEndpoint, packet);
            }
        }

        ThreadSafeQueue<Packet>& IncomingUDP() noexcept {
            return m_qUdpIncoming;
        }

        void UpdateDual(size_t maxMessages = SIZE_MAX) {
            this->Update(maxMessages);

            size_t count = 0;
            Packet pkt;
            while (count < maxMessages && m_qUdpIncoming.Pop(pkt)) {
                OnUDPMessage(pkt);
                count++;
            }
        }

    protected:
        virtual void OnUDPMessage(Packet&) {}

    protected:
        std::shared_ptr<UDPChannel> m_udpChannel;
        asio::ip::udp::endpoint m_serverUdpEndpoint;
        ThreadSafeQueue<Packet> m_qUdpIncoming;
    };

} // namespace darkfire::net
