package io.qdb.buffer;

import java.io.IOException;

/**
 * Queue that supports sequential retrieval of old messages i.e. it also behaves like a list.
 */
public interface MessageBuffer {

    /**
     * Add a message to the buffer and return its unique id. The id will always be bigger than the id of any
     * previously added messages.
     */
    long add(byte[] data, int offset, int length) throws IOException;

    /**
     * Get a message from the buffer by id. If id is 0 then the oldest message (smallest id) is returned. If id is -1
     * then the newest message (biggest id) is returned. If the buffer is empty null is returned.
     */
    Msg get(long id);

    /**
     * Remove all messages with ID less than or equal to id from the buffer.
     */
    void remove(long id);

    /**
     * A message retrieved from the buffer.
     */
    interface Msg {

        /**
         * The unique ID of the message.
         */
        long getId();

        /**
         * When was the message added to the buffer?
         */
        long getTimestamp();

        /**
         * What is the size of the message in bytes?
         */
        int size();

        /**
         * Get the message data into a new byte[] array.
         */
        byte[] getBytes();
    }

}
