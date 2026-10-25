#include <darkfire/darkfire.hpp>
#include <cassert>
#include <iostream>
#include <thread>

struct Vector3 {
    float x = 0.0f;
    float y = 0.0f;
    float z = 0.0f;

    static Vector3 Lerp(const Vector3& a, const Vector3& b, double t) {
        float f = static_cast<float>(t);
        return {
            a.x + (b.x - a.x) * f,
            a.y + (b.y - a.y) * f,
            a.z + (b.z - a.z) * f
        };
    }

    Vector3 operator+(const Vector3& other) const {
        return { x + other.x, y + other.y, z + other.z };
    }

    Vector3 operator-(const Vector3& other) const {
        return { x - other.x, y - other.y, z - other.z };
    }

    Vector3 operator*(float s) const {
        return { x * s, y * s, z * s };
    }

    friend Vector3 operator*(float s, const Vector3& v) {
        return v * s;
    }
};

void TestRTTTracker() {
    std::cout << "  [TEST] RTTTracker EWMA Calculation... ";

    darkfire::net::game::RTTTracker clientTracker;
    assert(!clientTracker.IsInitialized());

    // Generate Ping
    auto ping = clientTracker.CreatePing(1);
    assert(ping.HasFlag(darkfire::net::PacketFlags::HeartbeatPing));

    // Simulate minor delay
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    // Server receives and creates Pong
    auto pong = darkfire::net::game::RTTTracker::CreatePong(ping);
    assert(pong.HasFlag(darkfire::net::PacketFlags::HeartbeatPong));

    // Simulate return trip delay
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    // Client processes Pong
    clientTracker.ProcessPong(pong);

    assert(clientTracker.IsInitialized());
    double rtt = clientTracker.GetRTT();
    assert(rtt >= 30.0 && rtt <= 100.0); // Should be roughly ~40ms

    std::cout << "PASSED (Estimated RTT: " << rtt << " ms, Jitter: " << clientTracker.GetJitter() << " ms)\n";
}

void TestSnapshotHistoryInterpolation() {
    std::cout << "  [TEST] SnapshotHistory Interpolation & Clamping... ";

    darkfire::net::game::SnapshotHistory<Vector3, 16> history;
    assert(history.Empty());

    // Add snapshots at t=1000, 2000, 3000 ms
    history.AddSnapshot(1000, Vector3{ 0.0f, 0.0f, 0.0f });
    history.AddSnapshot(2000, Vector3{ 10.0f, 20.0f, 0.0f });
    history.AddSnapshot(3000, Vector3{ 30.0f, 40.0f, 0.0f });

    assert(history.Count() == 3);

    auto lerp = [](const Vector3& a, const Vector3& b, double t) {
        return Vector3::Lerp(a, b, t);
    };

    // Sample at t = 1500 (halfway between t=1000 and t=2000)
    auto s1500 = history.Sample(1500, lerp);
    assert(s1500.has_value());
    assert(std::abs(s1500->x - 5.0f) < 0.001f);
    assert(std::abs(s1500->y - 10.0f) < 0.001f);

    // Sample at t = 2500 (halfway between t=2000 and t=3000)
    auto s2500 = history.Sample(2500, lerp);
    assert(s2500.has_value());
    assert(std::abs(s2500->x - 20.0f) < 0.001f);
    assert(std::abs(s2500->y - 30.0f) < 0.001f);

    // Sample beyond newest: t = 4000 -> clamps to t=3000
    auto s4000 = history.Sample(4000, lerp);
    assert(s4000.has_value());
    assert(s4000->x == 30.0f);

    // Sample before oldest: t = 500 -> clamps to t=1000
    auto s500 = history.Sample(500, lerp);
    assert(s500.has_value());
    assert(s500->x == 0.0f);

    std::cout << "PASSED\n";
}

void TestNetworkSimulator() {
    std::cout << "  [TEST] NetworkSimulator Artificial Latency & Packet Loss... ";

    darkfire::net::game::NetworkSimulator sim;
    sim.SetLatency(30, 50); // 30 - 50 ms
    sim.SetPacketLoss(0.20); // 20% loss

    constexpr int Total = 100;
    int enqueued = 0;
    for (int i = 0; i < Total; ++i) {
        darkfire::net::Packet pkt(1);
        pkt << i;
        if (sim.Enqueue(pkt)) {
            enqueued++;
        }
    }

    assert(sim.DroppedCount() > 0); // With 20% loss over 100 packets, drop count > 0
    assert(enqueued == Total - sim.DroppedCount());

    // Initially immediately after enqueue, 0 should be ready (since min latency is 30ms)
    auto readyInitial = sim.PollReady();
    assert(readyInitial.empty());

    // Wait 60ms
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    auto readyLater = sim.PollReady();
    assert(readyLater.size() == static_cast<size_t>(enqueued));

    std::cout << "PASSED (Sent: " << Total << ", Dropped: " << sim.DroppedCount() << ", Delivered: " << readyLater.size() << ")\n";
}

void TestSequenceWrapAroundAndUnreliableSequenced() {
    std::cout << "  [TEST] RFC 1982 Sequence Wrap-Around & UnreliableSequenced Drop Filter... ";

    // 16-bit wrap-around safe arithmetic
    assert(darkfire::net::sequence_greater_than<uint16_t>(1, 0));
    assert(!darkfire::net::sequence_greater_than<uint16_t>(0, 1));
    assert(darkfire::net::sequence_greater_than<uint16_t>(0, 65535)); // Wrap-around! 0 is greater than 65535
    assert(!darkfire::net::sequence_greater_than<uint16_t>(65535, 0));
    assert(darkfire::net::sequence_greater_than<uint16_t>(100, 65500));
    assert(darkfire::net::sequence_less_than<uint16_t>(65500, 100));
    assert(darkfire::net::sequence_greater_or_equal<uint16_t>(50, 50));
    assert(darkfire::net::sequence_less_or_equal<uint16_t>(50, 50));

    // 32-bit wrap-around safe arithmetic
    assert(darkfire::net::sequence_greater_than<uint32_t>(1, 0));
    assert(!darkfire::net::sequence_greater_than<uint32_t>(0, 1));
    assert(darkfire::net::sequence_greater_than<uint32_t>(0, 0xFFFFFFFFu)); // Wrap-around! 0 is greater than 2^32 - 1
    assert(!darkfire::net::sequence_greater_than<uint32_t>(0xFFFFFFFFu, 0));
    assert(darkfire::net::SequenceGreaterThan(0, 0xFFFFFFFFu));
    assert(darkfire::net::sequence_greater_than<uint32_t>(10, 0xFFFFFFF0u));
    assert(darkfire::net::sequence_less_than<uint32_t>(0xFFFFFFF0u, 10));

    // Channel-level sequence filtering per endpoint
    asio::io_context ioContext;
    darkfire::net::UDPChannel channel(ioContext);
    asio::ip::udp::endpoint peerA(asio::ip::make_address("127.0.0.1"), 15000);
    channel.SetEndpointSequenceFilter(peerA, true);

    // Initial packet seq = 100 -> accepted
    assert(channel.FilterIncomingSequence(peerA, 100));
    assert(channel.GetHighestReceivedSequence(peerA) == 100);

    // Stale packet seq = 99 -> dropped
    assert(!channel.FilterIncomingSequence(peerA, 99));

    // Duplicate packet seq = 100 -> dropped
    assert(!channel.FilterIncomingSequence(peerA, 100));

    // Newer packet seq = 105 -> accepted
    assert(channel.FilterIncomingSequence(peerA, 105));
    assert(channel.GetHighestReceivedSequence(peerA) == 105);

    // Stale packet seq = 103 -> dropped
    assert(!channel.FilterIncomingSequence(peerA, 103));

    // Sequence wrap-around for peerB
    asio::ip::udp::endpoint peerB(asio::ip::make_address("127.0.0.1"), 15001);
    channel.SetEndpointSequenceFilter(peerB, true);
    assert(channel.FilterIncomingSequence(peerB, 0xFFFFFFFEu));
    assert(channel.FilterIncomingSequence(peerB, 0xFFFFFFFFu));
    assert(channel.FilterIncomingSequence(peerB, 0x00000000u)); // Wrap-around!
    assert(!channel.FilterIncomingSequence(peerB, 0xFFFFFFFFu)); // Stale after wrap!
    assert(!channel.FilterIncomingSequence(peerB, 0x00000000u)); // Duplicate after wrap!
    assert(channel.FilterIncomingSequence(peerB, 0x00000001u)); // Newer after wrap!

    std::cout << "PASSED\n";
}

void TestNTPClockSyncAndOutlierRejection() {
    std::cout << "  [TEST] NTP Clock Sync Outlier Rejection & Drift Scaling... ";

    darkfire::net::game::clock_sync tracker(0.2, 0.25);
    assert(!tracker.IsInitialized());

    // Baseline conditions:
    // Client-Server clock offset: +500,000 us (Server clock is 500ms ahead)
    // Uplink latency: 15,000 us (15 ms)
    // Downlink latency: 35,000 us (35 ms)
    // Server processing time: 2,000 us (2 ms)
    // Round-trip network time: 50,000 us (50 ms)
    constexpr int64_t TrueOffset = 500000;
    constexpr uint64_t Uplink = 15000;
    constexpr uint64_t Downlink = 35000;
    constexpr uint64_t ServerProc = 2000;

    // Send 10 normal baseline samples
    for (int i = 0; i < 10; ++i) {
        uint64_t t0 = 1000000ULL + i * 50000ULL;
        uint64_t t1 = static_cast<uint64_t>(static_cast<int64_t>(t0 + Uplink) + TrueOffset);
        uint64_t t2 = t1 + ServerProc;
        uint64_t t3 = t0 + Uplink + ServerProc + Downlink;

        tracker.add_sample(t0, t1, t2, t3);
    }

    assert(tracker.IsInitialized());
    double rtt = tracker.GetRTT();
    assert(std::abs(rtt - 50.0) < 5.0); // Close to 50ms
    int64_t smoothedOffset = tracker.clock_offset_us();
    assert(std::abs(smoothedOffset - 490000) < 15000);

    // Now inject 3 extreme network spike outliers (e.g. 350ms RTT spike caused by bufferbloat)
    for (int i = 0; i < 3; ++i) {
        uint64_t t0 = 2000000ULL + i * 50000ULL;
        uint64_t t1 = static_cast<uint64_t>(static_cast<int64_t>(t0 + Uplink) + TrueOffset);
        uint64_t t2 = t1 + ServerProc;
        uint64_t t3 = t0 + Uplink + ServerProc + 300000ULL; // 300ms return delay!

        tracker.add_sample(t0, t1, t2, t3);
    }

    // Outlier rejection filter must protect smoothed offset from jumping down by ~150ms!
    int64_t postSpikeOffset = tracker.clock_offset_us();
    assert(std::abs(postSpikeOffset - smoothedOffset) < 5000); // Smoothed offset did not get corrupted!

    // Verify estimated_server_time_us
    uint64_t localTime = 5000000ULL;
    uint64_t serverEst = tracker.estimated_server_time_us(localTime);
    assert(static_cast<int64_t>(serverEst - localTime) == tracker.clock_offset_us());

    // Verify time_drift_scale
    float driftScale = tracker.time_drift_scale();
    assert(driftScale >= 0.95f && driftScale <= 1.05f);

    std::cout << "PASSED (Smoothed Offset: " << tracker.clock_offset_us() << " us, Drift Scale: " << driftScale << ")\n";
}

struct PlayerInput {
    float moveX = 0.0f;
    float moveY = 0.0f;
    uint16_t buttons = 0;

    bool operator==(const PlayerInput& o) const = default;
};

struct PlayerState {
    float posX = 0.0f;
    float posY = 0.0f;
};

void TestRedundantInputHistoryAndBundleRecovery() {
    std::cout << "  [TEST] Generic Redundant Input Window & Wire Bundle Gap Recovery... ";

    darkfire::net::game::input_ring_buffer<PlayerInput, PlayerState, 32> buffer;
    assert(buffer.empty());

    // Client records inputs and predictions for ticks 1..10
    for (uint32_t t = 1; t <= 10; ++t) {
        PlayerInput input{ static_cast<float>(t) * 1.0f, static_cast<float>(t) * 2.0f, static_cast<uint16_t>(t) };
        PlayerState state{ static_cast<float>(t) * 10.0f, static_cast<float>(t) * 20.0f };
        buffer.push(t, input, state);
    }

    assert(buffer.size() == 10);
    assert(buffer.latest_tick() == 10);
    assert(buffer.oldest_tick() == 1);
    assert(buffer.has(5));

    // Rollback reconciliation: update predicted state at tick 5
    assert(buffer.update_predicted_state(5, PlayerState{ 99.0f, 99.0f }));
    auto entry5 = buffer.get(5);
    assert(entry5.has_value());
    assert(entry5->predicted_state.posX == 99.0f);

    // Acknowledge up to tick 4
    buffer.acknowledge_up_to(4);
    assert(buffer.oldest_tick() == 5);
    assert(!buffer.has(4));
    assert(buffer.has(5));
    assert(buffer.size() == 6); // 5..10

    // Build redundant bundle with 4 historical inputs ending at tick 10
    auto bundle10 = buffer.build_bundle<4>(10);
    assert(bundle10.count == 4);
    assert(bundle10.latest_tick == 10);
    assert(bundle10.oldest_tick() == 7); // inputs 7, 8, 9, 10

    // Wire serialization test with byte_writer and byte_reader
    darkfire::net::byte_writer writer;
    writer << bundle10;
    assert(writer.Size() > 0);

    darkfire::net::byte_reader reader(writer.Span());
    darkfire::net::game::redundant_input_bundle<PlayerInput, 4> unpacked;
    reader >> unpacked;
    assert(unpacked.latest_tick == 10);
    assert(unpacked.count == 4);
    assert(unpacked.oldest_tick() == 7);

    // Check recovered values
    for (uint32_t t = 7; t <= 10; ++t) {
        auto inOpt = unpacked.get_input_for_tick(t);
        assert(inOpt.has_value());
        assert(inOpt->buttons == static_cast<uint16_t>(t));
    }

    // Packet drop simulation:
    // Server has only processed inputs up to tick 102.
    // Packets 103 and 104 are DROPPED over UDP!
    // Packet 105 arrives containing redundant inputs [102, 103, 104, 105].
    darkfire::net::game::redundant_input_bundle<PlayerInput, 4> bundle105;
    for (uint32_t t = 102; t <= 105; ++t) {
        bundle105.add_input(t, PlayerInput{ static_cast<float>(t), static_cast<float>(t), static_cast<uint16_t>(t) });
    }

    uint32_t serverLastProcessedTick = 102;
    std::vector<uint32_t> recoveredTicks;

    // Server recovers missing inputs without gaps
    for (uint32_t t = bundle105.oldest_tick(); t <= bundle105.latest_tick; ++t) {
        if (t > serverLastProcessedTick) {
            auto in = bundle105.get_input_for_tick(t);
            assert(in.has_value());
            recoveredTicks.push_back(t);
            serverLastProcessedTick = t;
        }
    }

    assert(recoveredTicks.size() == 3);
    assert(recoveredTicks[0] == 103);
    assert(recoveredTicks[1] == 104);
    assert(recoveredTicks[2] == 105);
    assert(serverLastProcessedTick == 105);

    std::cout << "PASSED\n";
}

struct EntityTransform {
    Vector3 pos;
    Vector3 vel;
};

void TestSnapshotHistoryCustomAndHermiteSpline() {
    std::cout << "  [TEST] SnapshotHistory Custom Callable & Hermite Spline Interpolation... ";

    darkfire::net::game::snapshot_history<EntityTransform, 16> history(0.001f); // 1ms scale
    assert(history.empty());

    // Snapshot at t=1000ms: pos=(0,0,0), vel=(10,0,0), ack_input_tick=50
    history.add_snapshot(1000, EntityTransform{ Vector3{ 0.0f, 0.0f, 0.0f }, Vector3{ 10.0f, 0.0f, 0.0f } }, 50);
    // Snapshot at t=2000ms: pos=(10,0,0), vel=(10,0,0), ack_input_tick=100
    history.add_snapshot(2000, EntityTransform{ Vector3{ 10.0f, 0.0f, 0.0f }, Vector3{ 10.0f, 0.0f, 0.0f } }, 100);

    // Verify reconciliation metadata
    assert(history.last_acknowledged_input_tick() == 100);

    // 1. Custom 4-arg callable Hermite spline sampling at t=1500 (halfway, t=0.5, dt=1.0s)
    EntityTransform sampledState{};
    bool sampled = history.sample(1500, sampledState, [](const EntityTransform& a, const EntityTransform& b, float t, float dt_sec) {
        EntityTransform out;
        out.pos = darkfire::net::game::hermite_interpolate(a.pos, a.vel, b.pos, b.vel, t, dt_sec);
        out.vel = Vector3::Lerp(a.vel, b.vel, t);
        return out;
    });

    assert(sampled);
    // Constant velocity: at t=0.5s, pos.x should be exactly 5.0
    assert(std::abs(sampledState.pos.x - 5.0f) < 0.001f);
    assert(std::abs(sampledState.vel.x - 10.0f) < 0.001f);

    // 2. Pure Hermite spline cubic S-curve test:
    // With zero velocities at both ends (ease-in ease-out):
    // p0 = 0, v0 = 0, p1 = 10, v1 = 0, dt = 1.0s
    // At t = 0.25: h00(0.25)*0 + h01(0.25)*10 = 0.15625 * 10 = 1.5625 (smooth acceleration, not 2.5)
    Vector3 p0{ 0.0f, 0.0f, 0.0f };
    Vector3 v0{ 0.0f, 0.0f, 0.0f };
    Vector3 p1{ 10.0f, 0.0f, 0.0f };
    Vector3 v1{ 0.0f, 0.0f, 0.0f };

    Vector3 pAtQuarter = darkfire::net::game::hermite_interpolate(p0, v0, p1, v1, 0.25f, 1.0f);
    assert(std::abs(pAtQuarter.x - 1.5625f) < 0.001f);

    Vector3 pAtHalf = darkfire::net::game::hermite_interpolate(p0, v0, p1, v1, 0.50f, 1.0f);
    assert(std::abs(pAtHalf.x - 5.0f) < 0.001f);

    Vector3 pAtThreeQuarters = darkfire::net::game::hermite_interpolate(p0, v0, p1, v1, 0.75f, 1.0f);
    assert(std::abs(pAtThreeQuarters.x - 8.4375f) < 0.001f);

    std::cout << "PASSED\n";
}

void RunGamePrimitivesTests() {
    std::cout << "\n--- Running Game Networking Primitives Tests ---\n";
    TestRTTTracker();
    TestSnapshotHistoryInterpolation();
    TestNetworkSimulator();
    TestSequenceWrapAroundAndUnreliableSequenced();
    TestNTPClockSyncAndOutlierRejection();
    TestRedundantInputHistoryAndBundleRecovery();
    TestSnapshotHistoryCustomAndHermiteSpline();
}
