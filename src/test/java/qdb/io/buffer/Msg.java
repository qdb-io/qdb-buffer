package qdb.io.buffer;

import java.util.Random;

class Msg {
    long id;
    long timestamp;
    String routingKey;
    byte[] payload;

    public Msg(long timestamp, Random rnd, int maxPayloadSize) {
        this.timestamp = timestamp;
        routingKey = "key" + timestamp;
        payload = new byte[rnd.nextInt(maxPayloadSize + 1)];
        rnd.nextBytes(payload);
    }

    Msg(long timestamp, String routingKey, byte[] payload) {
        this.timestamp = timestamp;
        this.routingKey = routingKey;
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "id:" + id + " timestamp:" + timestamp;
    }
}
