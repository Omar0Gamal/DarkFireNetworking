#pragma once

// DarkFireNetworking v2 - C++20 Dual-Transport Real-Time Networking Framework

#include "net/error.hpp"
#include "net/log.hpp"
#include "net/endian.hpp"
#include "net/buffer_view.hpp"
#include "net/byte_writer.hpp"
#include "net/byte_reader.hpp"
#include "net/binary_stream.hpp"
#include "net/security.hpp"
#include "net/lockfree_queue.hpp"
#include "net/packet.hpp"
#include "net/tcp_connection.hpp"
#include "net/tcp_client.hpp"
#include "net/tcp_server.hpp"
#include "net/udp_channel.hpp"
#include "net/dual_client.hpp"
#include "net/dual_server.hpp"

// Game Networking Primitives
#include "net/game/rtt_tracker.hpp"
#include "net/game/input_history.hpp"
#include "net/game/snapshot_history.hpp"
#include "net/game/network_simulator.hpp"
