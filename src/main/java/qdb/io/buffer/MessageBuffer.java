package qdb.io.buffer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Queue that supports sequential retrieval of old messages by id and timestamp.
 */
public interface MessageBuffer extends Closeable {

    /**
     * Append a message and return its id.
     */
    long append(long timestamp, String routingKey, ByteBuffer payload) throws IOException;

    /**
     * What ID will the next message appended have?
     */
    long getNextMessageId() throws IOException;

    /**
     * Create a cursor reading data from messageId onwards. To read the oldest message use 0 as the message ID. To
     * read the newest use {@link #getNextMessageId()}. If the messageId is before the oldest message the the cursor
     * reads from the oldest message onwards. The cursor should only be used from one thread at a time i.e. it is not
     * thread safe.
     */
    MessageCursor cursor(long messageId) throws IOException;

    /**
     * Create a cursor reading data from timestamp onwards. If timestamp is before the first message then the cursor
     * reads starting at the first message. If timestamp is past the last message then the cursor will return false
     * until more messages appear in the buffer. The cursor should only be used from one thread at a time i.e. it is
     * not thread safe.
     */
    MessageCursor cursorByTimestamp(long timestamp) throws IOException;

    /**
     * Set the maximum size of this buffer in bytes. When it is full the oldest messages are deleted to make space.
     * Use zero for unlimited size.
     */
    void setMaxLength(long bytes) throws IOException;

    /**
     * What is the maximum size of this buffer in bytes?
     */
    long getMaxLength();

    /**
     * How much space is this buffer currently consuming in bytes?
     */
    long getLength();

    /**
     * Sync all changes to persistent storage. A system crash immediately following this call will not result in
     * any loss of messages.
     */
    void sync() throws IOException;
}
