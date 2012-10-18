package io.qdb.buffer;

import java.io.IOException;

/**
 * Iterate over messages in a forward direction.
 */
public interface MessageCursor {

    /**
     * Advance to the next message or return false if there are no more messages. The cursor initially starts
     * "before" the first message (if any).
     */
    public boolean next() throws IOException;

    /**
     * Get the ID of the current message.
     */
    public long getId() throws IOException;

    /**
     * Get the timestamp of the current message.
     */
    public long getTimestamp() throws IOException;

    /**
     * Get the routing key of the current message.
     */
    public String getRoutingKey() throws IOException;

    /**
     * Get the size in bytes of the payload of the current message.
     */
    public int getPayloadSize() throws IOException;

    /**
     * Get the payload of the current message.
     */
    public byte[] getPayload() throws IOException;
}