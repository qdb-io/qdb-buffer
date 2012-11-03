package qdb.io.buffer;

/**
 * A point on the message timeline for a buffer.
 */
public interface Timeline {

    /**
     * How many points are on this timeline?
     */
    int size();

    /**
     * Get the id of the message at i on the timeline.
     */
    long getMessageId(int i);

    /**
     * Get the timestamp of the message at i on the timeline. Note that this may be rounded down to the nearest
     * second.
     */
    long getTimestamp(int i);

    /**
     * Return the number of bytes of messages between i and i + 1 on the timeline.
     */
    int getBytes(int i);

    /**
     * Return the time between i and i + 1 on the timeline.
     */
    long getMillis(int i);

    /**
     * Return the number of messages between i and i + 1 on the timeline. Note that this will return -1 if this
     * unknown.
     */
    long getCount(int i);

}
