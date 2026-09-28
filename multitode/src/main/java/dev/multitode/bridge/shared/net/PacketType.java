package dev.multitode.bridge.shared.net;

public enum PacketType {
    HELLO(1),
    HELLO_ACCEPTED(2),
    HELLO_REJECTED(3),
    DISCONNECT(4),
    PING(5),
    LUA_MESSAGE(6);

    private final int id;

    PacketType(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    /** @return the type, or {@code null} when the id is not one of ours. */
    public static PacketType fromId(int id) {
        for (PacketType value : values()) {
            if (value.id == id) {
                return value;
            }
        }

        // Unknown id = a peer speaking a different protocol version, or a corrupt
        // stream. The session loops only know how to handle IOException, so an
        // unchecked throw here escaped their catch blocks and killed the connection
        // thread outside the reconnect path. Callers treat null as a protocol error.
        return null;
    }
}
