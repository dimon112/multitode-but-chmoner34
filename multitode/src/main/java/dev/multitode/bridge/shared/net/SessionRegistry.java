package dev.multitode.bridge.shared.net;

import com.prineside.tdi2.utils.logging.TLog;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class SessionRegistry {
    private static final TLog LOGGER = TLog.forTag("multitode/SessionRegistry");

    /**
     * Lua drains at most a fixed budget per render callback, so a peer that sends
     * faster than that would otherwise grow this queue without bound. Also a
     * plain robustness limit: any burst after a stall (level load, window drag,
     * long GC) queues faster than it drains.
     */
    private static final int MAX_PENDING_LUA_MESSAGES = 4096;
    private static final int DROP_LOG_EVERY = 100;

    private final LocalSessionInfo localSessionInfo = new LocalSessionInfo();
    private final Map<Integer, PeerInfo> peersByPlayerId = new ConcurrentHashMap<>();
    private final Queue<InboundLuaMessage> inboundLuaMessages = new ConcurrentLinkedQueue<>();
    private final AtomicLong droppedInboundLuaMessages = new AtomicLong();
    private final AtomicLong lastDropWarningAt = new AtomicLong();

    public LocalSessionInfo getLocalSessionInfo() {
        return localSessionInfo;
    }

    public void putPeer(PeerInfo peerInfo) {
        peersByPlayerId.put(peerInfo.getPlayerId(), peerInfo);
    }

    public PeerInfo getPeer(int playerId) {
        return peersByPlayerId.get(playerId);
    }

    public void removePeer(int playerId) {
        peersByPlayerId.remove(playerId);
    }

    public int getConnectedPeerCount() {
        // Exclude our own loopback connection: on HOST_AND_CLIENT the host's
        // own client registers as a peer, but it must not count as a remote peer.
        int localPlayerId = localSessionInfo.getLocalPlayerId();
        if (localPlayerId == 0) {
            return peersByPlayerId.size();
        }

        int count = 0;
        for (Integer playerId : peersByPlayerId.keySet()) {
            if (playerId != null && playerId != localPlayerId) {
                count++;
            }
        }
        return count;
    }

    public List<PeerInfo> getPeers() {
        List<PeerInfo> peers = new ArrayList<>(peersByPlayerId.values());
        peers.sort(Comparator.comparingInt(PeerInfo::getPlayerId));
        return peers;
    }

    public String describePeers() {
        List<PeerInfo> peers = getPeers();
        if (peers.isEmpty()) {
            return "no connected peers";
        }

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < peers.size(); i++) {
            if (i > 0) {
                builder.append(" | ");
            }
            builder.append(peers.get(i).describe());
        }
        return builder.toString();
    }

    /**
     * Non-blocking, never rejects the caller: on overflow the newest message is
     * dropped so the network read thread is never stalled by a slow consumer.
     */
    public void enqueueInboundLuaMessage(InboundLuaMessage message) {
        if (inboundLuaMessages.size() >= MAX_PENDING_LUA_MESSAGES) {
            long total = droppedInboundLuaMessages.incrementAndGet();
            long now = System.currentTimeMillis();
            long last = lastDropWarningAt.get();
            // Rate-limited so a flood cannot turn the warning itself into the load.
            if (total % DROP_LOG_EVERY == 1 && now - last > 1000L
                    && lastDropWarningAt.compareAndSet(last, now)) {
                LOGGER.w("Inbound Lua message queue full (cap %d); dropped %d so far",
                        MAX_PENDING_LUA_MESSAGES, total);
            }
            return;
        }
        inboundLuaMessages.add(message);
    }

    public InboundLuaMessage pollInboundLuaMessage() {
        return inboundLuaMessages.poll();
    }

    public int getPendingLuaMessageCount() {
        return inboundLuaMessages.size();
    }

    public long getDroppedInboundLuaMessageCount() {
        return droppedInboundLuaMessages.get();
    }

    public String describe() {
        return "sessionId=" + localSessionInfo.getSessionId()
                + ", localPlayerId=" + localSessionInfo.getLocalPlayerId()
                + ", connectionState=" + localSessionInfo.getConnectionState().name()
                + ", remoteAddress=" + localSessionInfo.getRemoteAddress()
                + ", connectedPeerCount=" + getConnectedPeerCount();
    }
}
