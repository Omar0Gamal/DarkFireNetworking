# DarkFireNetworking

[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Build: CMake](https://img.shields.io/badge/Build-CMake%20%7C%20Premake5-orange.svg)](https://cmake.org)

A modern, zero-copy, dual-transport (TCP + UDP) real-time multiplayer networking framework for C++20.

Originally based on the OneLoneCoder/javidx9 Asio networking tutorial architecture, this project is a complete rewrite engineered for production game networking: replacing callback chains with **C++20 coroutines**, providing **forward FIFO zero-copy binary serialization**, a **sequenced and reliable UDP transport** with sliding-window ACKs and MTU fragmentation, **HMAC-SHA256 handshake authentication**, **lock-free ring buffers**, and **game networking primitives** (EWMA RTT tracking, snapshot interpolation, network simulator).

---

## Architecture

```mermaid
flowchart TD
    subgraph App["Game & Simulation Layer"]
        Logic["Game Simulation / Main Thread Tick"]
        Interp["SnapshotHistory (Interpolation / Extrapolation)"]
        RTT["RTT & Jitter Tracker (EWMA)"]
    end

    subgraph Core["DarkFireNetworking Engine"]
        subgraph Serialization["Serialization Engine"]
            BW["ByteWriter / BinaryWriter"]
            BR["ByteReader / BinaryReader"]
            BV["BufferView / std::span (Zero-Copy)"]
        end

        subgraph Concurrency["Concurrency & Queues"]
            SPSC["Lock-Free SPSC RingBuffer (alignas 64)"]
            MPSC["Lock-Free MPSC Queue (Vyukov Node Queue)"]
            TSQ["ThreadSafeQueue (Blocking CV Queue)"]
        end

        subgraph Transport["Dual-Transport Layer"]
            TCP["TCP Stream Engine (Framing, Keepalive, HMAC)"]
            UDP["UDP Channel (Sequencing, Sliding Window ACK, Fragmentation)"]
            Sim["NetworkConditionSimulator (Latency, Loss, Jitter)"]
        end

        subgraph Coroutines["Asio C++20 Coroutines"]
            CoroRead["co_await async_read / async_receive"]
            CoroWrite["co_await async_write / async_send"]
        end
    end

    subgraph OS["OS Sockets"]
        TCP_Sock["TCP Socket (TCP_NODELAY)"]
        UDP_Sock["UDP Datagram Socket"]
    end

    Logic --> Interp
    Logic --> RTT
    Logic --> BW
    Logic --> BR
    BW --> TCP
    BW --> UDP
    TCP --> CoroWrite --> TCP_Sock
    UDP --> Sim --> UDP_Sock
    TCP_Sock --> CoroRead --> SPSC --> Logic
    UDP_Sock --> CoroRead --> UDP --> Logic
```

---

## Architectural Comparison

| Feature | Legacy DarkFire (javidx9) | DarkFire 2.0 |
| :--- | :--- | :--- |
| **Language Standard** | C++11 / C++14 | **C++20** |
| **Serialization Order** | LIFO inverted (pops backwards from tail) | **FIFO forward** (what is written first is read first) |
| **Bounds Safety** | None (`size - sizeof` underflows `size_t`) | **Strict bounds checking**, returns `NetResult<T>` on error |
| **Endianness** | Host `memcpy` (breaks across architectures) | **Network Byte Order (Big-Endian)** via `<bit>` `std::endian` & `std::byteswap` |
| **Supported Types** | POD primitives only | Scalars, concept-constrained Enums, strings, vectors, trivially copyable structs |
| **Zero-Copy Support** | None (`std::vector` reallocations per push) | `std::span<const uint8_t>` and `BufferView` zero-copy slices |
| **Async Model** | Nested lambdas with raw pointer captures | **C++20 coroutines (`asio::awaitable<void>`, `co_await`)** |
| **Transport** | TCP only (subject to Head-of-Line blocking) | **Dual-Transport (TCP + Fast Sequenced UDP + Reliable UDP with RFC 1982 wrap-around filtering)** |
| **UDP Fragmentation** | Not available | **MTU fragmentation and reassembly** (payloads up to 64 KB sliced into $\le 1200$ B chunks) |
| **UDP Reliability** | Not available | **Sliding 32-bit ACK history bitfield** with selective retransmission |
| **Authentication** | Basic arithmetic integer scramble | **HMAC-SHA256 challenge-response**, CRC32 integrity checks, magic byte framing |
| **Queues** | Mutex + CV `net_tsqueue` contention | **Cache-line aligned SPSC ring buffer**, **lock-free MPSC node queue** |
| **Game Utilities** | None | **NTP clock sync with outlier rejection**, **client prediction input ring buffer**, **redundant wire bundle gap recovery**, **Hermite spline snapshot interpolation**, **reconciliation ack ticks**, **network simulator** |
| **Diagnostics** | Direct `std::cout` prints | **Pluggable leveled logger (`Trace`, `Debug`, `Info`, `Warning`, `Error`)** |
| **Build System** | Visual Studio `.vcxproj` only | Target-based **CMake 3.20+** and **Premake5** |

---

## Packet Container & Wire Protocol

The core data abstraction is `darkfire::net::Packet`, a unified container that pairs a transport wire header with a dynamic, zero-copy byte buffer:

```
                    +----------------------------------------------+
                    |            Packet Class Container            |
                    |   (Unified C++20 Memory & Transport View)    |
                    +----------------------++----------------------+
                                           ||
             +-----------------------------++----------------------------+
             |                                                           |
             v                                                           v
+-------------------------------+                       +-------------------------------+
|     Transport Wire Header     |                       |       Dynamic Payload         |
+-------------------------------+                       +-------------------------------+
| TCPHeader (14 B, #pragma pack)|                       | ByteWriter (FIFO Stream)      |
|          -- OR --             |                       | std::vector<uint8_t> buffer   |
| UDPHeader (30 B, #pragma pack)|                       | std::span / BufferView        |
| (Magic, ID, Sequence, CRC32)  |                       | (Zero-copy binary slices)     |
+-------------------------------+                       +-------------------------------+
             |                                                           |
             +-----------------------------++----------------------------+
                                           ||
                        +------------------vv-----------------+
                        |   Streaming Operators & Helpers     |
                        |     packet << x << y << str;        |
                        |     packet.FinalizeTCP() / UDP()    |
                        |     packet.ValidateChecksum()       |
                        +-------------------------------------+
```

All multi-byte integer and float fields in wire headers are transmitted in **Network Byte Order (Big-Endian)**.

---

### TCP Wire Framing (14-Byte Header)

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|         Magic (0xDF4E)        |    Version    |     Flags     |  0x00 - 0x03
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|          Message ID           |      Body Size (High 16)      |  0x04 - 0x07
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      Body Size (Low 16)       |       Checksum (High 16)      |  0x08 - 0x0B
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|       Checksum (Low 16)       |       Payload Data ...        |  0x0C - 0x0F
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
:                                                               :
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| Offset | Field | Type | Size | Description |
| :---: | :--- | :---: | :---: | :--- |
| `0x00` | `magic` | `uint16_t` | 2 B | Magic framing identifier (`0xDF4E`) |
| `0x02` | `version` | `uint8_t` | 1 B | Protocol version (`0x02`) |
| `0x03` | `flags` | `uint8_t` | 1 B | Control flags (`Handshake`, `Ping/Pong`, `System`) |
| `0x04` | `message_id` | `uint16_t` | 2 B | Application message identifier / enum |
| `0x06` | `body_size` | `uint32_t` | 4 B | Payload size in bytes (enforces $\le$ 64 KB limit) |
| `0x0A` | `checksum` | `uint32_t` | 4 B | IEEE 802.3 CRC32 computed over payload bytes |
| `0x0E` | `payload` | `uint8_t[]` | $N$ B | Forward FIFO serialized body |

---

### UDP Wire Framing (30-Byte Header)

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|         Magic (0xDF55)        |    Version    |     Flags     |  0x00 - 0x03
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|          Message ID           |     Sequence ID (High 16)     |  0x04 - 0x07
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     Sequence ID (Low 16)      |        Ack ID (High 16)       |  0x08 - 0x0B
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|        Ack ID (Low 16)        |     Ack Bitfield (High 16)    |  0x0C - 0x0F
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     Ack Bitfield (Low 16)     |        Fragment Index         |  0x10 - 0x13
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|        Fragment Count         |      Body Size (High 16)      |  0x14 - 0x17
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      Body Size (Low 16)       |       Checksum (High 16)      |  0x18 - 0x1B
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|       Checksum (Low 16)       |       Datagram Chunk ...      |  0x1C - 0x1F
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
:                                                               :
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| Offset | Field | Type | Size | Description |
| :---: | :--- | :---: | :---: | :--- |
| `0x00` | `magic` | `uint16_t` | 2 B | Magic datagram identifier (`0xDF55`) |
| `0x02` | `version` | `uint8_t` | 1 B | Protocol version (`0x02`) |
| `0x03` | `flags` | `uint8_t` | 1 B | Bitflags (`Reliable`, `Fragmented`, `Ping/Pong`) |
| `0x04` | `message_id` | `uint16_t` | 2 B | Application message identifier / enum |
| `0x06` | `sequence_id` | `uint32_t` | 4 B | Monotonically increasing datagram sequence ID |
| `0x0A` | `ack_id` | `uint32_t` | 4 B | Latest remote sequence ID acknowledged |
| `0x0E` | `ack_bitfield` | `uint32_t` | 4 B | 32-bit sliding window history of previous ACKs |
| `0x12` | `fragment_index` | `uint16_t` | 2 B | Zero-based slice index for MTU fragmentation |
| `0x14` | `fragment_count` | `uint16_t` | 2 B | Total fragment slices in payload (1 if unfragmented) |
| `0x16` | `body_size` | `uint32_t` | 4 B | Chunk payload size in bytes ($\le$ 1200 MTU) |
| `0x1A` | `checksum` | `uint32_t` | 4 B | IEEE 802.3 CRC32 of chunk payload |
| `0x1E` | `payload` | `uint8_t[]` | $N$ B | Raw datagram chunk |

---

## Integration

### CMake (`FetchContent` or `add_subdirectory`)

DarkFireNetworking is structured as a modern CMake interface library:

```cmake
# Option A: Submodule / subfolder
add_subdirectory(DarkFireNetworking)
target_link_libraries(MyGameEngine PRIVATE darkfire_net)

# Option B: FetchContent
include(FetchContent)
FetchContent_Declare(
    darkfire_net
    GIT_REPOSITORY https://github.com/Omar0Gamal/DarkFireNetworking.git
    GIT_TAG        main
)
FetchContent_MakeAvailable(darkfire_net)
target_link_libraries(MyGameEngine PRIVATE darkfire_net)
```

Target requirements:
- C++ standard: `cxx_std_20`
- Threading: `Threads::Threads`
- Windows socket libraries: `ws2_32`, `mswsock` (linked automatically on Windows)

---

## Quick Start

### 1. Binary Serialization (`ByteWriter` & `ByteReader`)

Streams read and write strictly in FIFO order, convert integers and floats to network byte order, and perform bounds checks on every read.

```cpp
#include <darkfire/darkfire.hpp>

enum class WeaponType : uint16_t { Rifle = 1, Rocket = 2 };

struct Transform {
    float x, y, z;
};
// Opt-in for zero-overhead memcpy serialization:
DARKFIRE_DECLARE_TRIVIALLY_SERIALIZABLE(Transform);

// Serialize
darkfire::net::ByteWriter writer;
writer << uint32_t(101) 
       << std::string("PlayerOne") 
       << WeaponType::Rifle 
       << Transform{ 10.0f, 20.0f, 30.0f };

// Deserialize
darkfire::net::ByteReader reader(writer.Span());
uint32_t id;
std::string name;
WeaponType weapon;
Transform pos;

reader >> id >> name >> weapon >> pos;

if (reader.IsOk()) {
    // Data successfully parsed
}
```

### 2. Dual-Transport Client & Server (TCP + UDP)

Use **TCP** for reliable handshakes, matchmaking, and chat. Use **UDP** for high-frequency position updates and input replication.

```cpp
#include <darkfire/darkfire.hpp>

enum class GameMsg : uint16_t {
    LoginRequest  = 1,
    LoginResponse = 2,
    PlayerInput   = 3,
    StateSnapshot = 4
};

// Server Implementation
class GameServer : public darkfire::net::DualServer<GameMsg> {
public:
    GameServer() : darkfire::net::DualServer<GameMsg>(60000 /*TCP*/, 60001 /*UDP*/) {}

protected:
    void OnMessage(std::shared_ptr<darkfire::net::TCPConnection> client, darkfire::net::Packet& packet) override {
        // Reliable TCP message
        if (packet.GetIdAs<GameMsg>() == GameMsg::LoginRequest) {
            darkfire::net::Packet response(GameMsg::LoginResponse);
            response << client->GetId();
            Send(client, response);
        }
    }

    void OnUDPMessage(const asio::ip::udp::endpoint& sender, darkfire::net::Packet& packet) override {
        // Fast UDP message (sequenced or reliable)
        if (packet.GetIdAs<GameMsg>() == GameMsg::PlayerInput) {
            // Process player input datagram
        }
    }
};

// In your game loop:
GameServer server;
server.StartDual();

while (gameRunning) {
    // Process incoming network messages on the main simulation thread
    server.UpdateDual();
    SimulateWorld();
}
```

### 3. Client-Side Prediction & Redundant Input Recovery

Maintain historical player inputs and predicted physics states for client-side prediction, server reconciliation, and seamless packet-loss recovery without retransmissions:

```cpp
#include <darkfire/darkfire.hpp>

struct MoveInput {
    float moveX = 0.0f;
    float moveY = 0.0f;
    uint16_t buttons = 0;
};

struct PlayerPhysicsState {
    float posX = 0.0f;
    float posY = 0.0f;
};

// 1. Client prediction buffer (Capacity: 128 ticks)
darkfire::net::game::input_ring_buffer<MoveInput, PlayerPhysicsState, 128> inputHistory;

// On each client simulation tick (e.g. tick 100):
inputHistory.push(currentTick, currentInput, predictedState);

// Pack redundant inputs (latest tick + 3 previous inputs) for wire transport:
auto bundle = inputHistory.build_bundle<4>(currentTick);

darkfire::net::ByteWriter writer;
writer << bundle; // Serializes tick 100 and inputs for ticks 97..100
client.SendUDPSequenced(packet);

// On server: recover dropped UDP packet gaps in tick order
darkfire::net::game::redundant_input_bundle<MoveInput, 4> incomingBundle;
reader >> incomingBundle;

for (uint32_t t = incomingBundle.oldest_tick(); t <= incomingBundle.latest_tick; ++t) {
    if (t > serverLastProcessedTick) {
        MoveInput in = *incomingBundle.get_input_for_tick(t);
        SimulatePlayer(in); // Seamlessly recovers dropped inputs without lag!
        serverLastProcessedTick = t;
    }
}
```

### 4. NTP-Style Clock Synchronization & Outlier Rejection

Synchronize client simulation time to server time using the 4-timestamp NTP formula with rolling-window outlier rejection (filtering out bufferbloat / queueing spikes):

```cpp
#include <darkfire/darkfire.hpp>

darkfire::net::game::clock_sync clockSync; // Alias for RTTTracker

// Ping/Pong exchange:
// Client sends ping with t0
// Server receives at t1, sends pong at t2
// Client receives at t3:
clockSync.add_sample(t0, t1, t2, t3);

// Smoothed clock offset in microseconds (positive if server is ahead)
int64_t offsetUs = clockSync.clock_offset_us();

// Accurate estimated server time for rendering / snapshot interpolation:
uint64_t currentServerTimeUs = clockSync.estimated_server_time_us(localTimeUs);

// Simulation pacing multiplier clamped to [0.95, 1.05]:
// Engine smoothly dilates dt to prevent clock snapping!
float pacingScale = clockSync.time_drift_scale();
float adjustedDt = baseDt * pacingScale;
```

### 5. Entity Snapshot Interpolation & Hermite Splines (`SnapshotHistory`)

Smooth remote player transforms and trajectories across variable packet arrival times using linear or cubic Hermite spline interpolation:

```cpp
#include <darkfire/darkfire.hpp>

struct EntityTransform {
    Vector3 pos;
    Vector3 vel;
};

darkfire::net::game::snapshot_history<EntityTransform, 64> history(0.001f); // 1ms scale

// On server snapshot arrival (piggybacking last acknowledged client input tick for reconciliation):
history.add_snapshot(packetTimestampMicros, incomingTransform, lastAckedInputTick);

// Reconciliation rollback check on client:
uint32_t confirmedInputTick = history.last_acknowledged_input_tick();
inputHistory.acknowledge_up_to(confirmedInputTick);

// On render frame: Hermite spline interpolation using position & velocity tangents
uint64_t renderTime = clockSync.GetSyncedServerTimeMicros() - 100'000; // 100ms interp delay
EntityTransform outState{};

bool sampled = history.sample(renderTime, outState, [](const EntityTransform& a, const EntityTransform& b, float t, float dt_sec) {
    EntityTransform interpolated;
    // Cubic Hermite spline curve P(t) = h00*p0 + h10*v0*dt + h01*p1 + h11*v1*dt
    interpolated.pos = darkfire::net::game::hermite_interpolate(a.pos, a.vel, b.pos, b.vel, t, dt_sec);
    interpolated.vel = Vector3::Lerp(a.vel, b.vel, t);
    return interpolated;
});

if (sampled) {
    renderEntity.SetPosition(outState.pos);
}
```

### 6. Unreliable-Sequenced UDP Delivery & RFC 1982 Serial Number Arithmetic

Drop stale out-of-order UDP datagrams automatically across sequence wrap-around boundaries (`0xFFFF -> 0x0000` or `0xFFFFFFFF -> 0x00000000`):

```cpp
#include <darkfire/darkfire.hpp>

// Safe sequence arithmetic (RFC 1982)
bool isNewer = darkfire::net::sequence_greater_than<uint32_t>(seqNew, seqOld);

// Enable automatic sequenced filtering per endpoint or channel-wide:
channel->SetUnreliableSequencedMode(true);

// Out-of-order and duplicate packets are discarded before callback/queue!
channel->SendSequenced(serverEp, positionPacket);
```

### 7. Network Condition Simulator

Test multiplayer replication locally under realistic latency, jitter, and packet loss:

```cpp
darkfire::net::game::NetworkSimulator sim;
sim.SetLatency(60, 100); // 60-100 ms latency
sim.SetPacketLoss(0.05);  // 5% random packet drop

sim.Enqueue(packet);

// In simulation tick:
for (auto& readyPacket : sim.PollReady()) {
    ProcessPacket(readyPacket);
}
```

---

## Building and Running Tests

### Requirements
- Compiler: MSVC 2022+ / Clang 14+ / GCC 11+ (C++20 support required)
- Build system: CMake 3.20+ or Premake5
- Standalone Asio 1.28+ (bundled under `Libs/asio-1.28.1`)

### Build Commands
```bash
# Configure
cmake -B build -G "Visual Studio 18 2026" -A x64

# Compile Release binaries
cmake --build build --config Release

# Run Unit & Integration Tests
ctest --test-dir build -C Release --output-on-failure
```

### Example Applications

1. **EchoBench** (`examples/EchoBench`):
   Stress benchmark verifying throughput and latency under high volume.
   ```bash
   .\build\examples\Release\EchoBench.exe
   ```
   *Performance*: Over 30,000 packets/sec on loopback with sub-millisecond round-trip times.

2. **ChatRoom** (`examples/ChatRoom`):
   Multi-client TCP broadcast room with handles and room IDs.
   ```bash
   .\build\examples\Release\ChatServer.exe 60020
   .\build\examples\Release\ChatClient.exe 127.0.0.1 60020
   ```

3. **SpatialReplication** (`examples/SpatialReplication`):
   Authoritative 60 Hz physics server replicating 2D/3D entity transforms over 20 Hz UDP snapshots with client-side interpolation.
   ```bash
   .\build\examples\Release\SpatialServer.exe
   .\build\examples\Release\SpatialClient.exe
   ```

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
