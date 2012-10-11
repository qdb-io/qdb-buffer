package io.qdb.buffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

/**
 * <p>A bunch of messages all in the same file. Supports detection and recovery from corruption due to server crash.
 * New messages are always appended to the end of the file. Thread safe.<p>
 *
 * <p>The first 4 bytes of the file hold its length at the last checkpoint. Recovery from a crash is simply a matter
 * of truncating the file to its last checkpoint length. That might discard some good messages but has the advantage
 * of being very fast (compared to calculating and checking message CRC values for example). The assumption is that
 * if the messages are very important they will be written to separate machines.</p>
 *
 * <p>The remainder of the file consists of records in the following format (all BIG_ENDIAN):</p>
 *
 * <pre>
 * record type: 1 byte (value always 0xA1 currently)
 * timestamp: 8 bytes
 * routing key size in bytes (m): 2 bytes
 * payload size (n): 4 bytes
 * routing key UTF8 encoded: m bytes
 * payload: n bytes
 * </pre>
 */
class MessageFile implements Closeable {

    private final File file;
    private final long baseOffset;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final ByteBuffer header;
    private final ByteBuffer[] srcs = new ByteBuffer[2];

    private int length;
    private int lastCheckpointLength;

    private static final byte TYPE_MESSAGE = (byte)0xA1;

    private static final Charset UTF8 = Charset.forName("UTF8");

    /**
     * Open a new or existing file.
     */
    public MessageFile(File file, long baseOffset) throws IOException {
        this.file = file;
        this.baseOffset = baseOffset;

        raf = new RandomAccessFile(file, "rw");
        channel = raf.getChannel();
        header = ByteBuffer.allocateDirect(2048);
        srcs[0] = header;

        int size = (int)channel.size();
        if (size == 0) {
            length = 4; // assume checkpoint is in file already
        } else { // use checkpoint to recover file
            header.limit(4);
            int read = channel.read(header);
            if (read != 4) throw new IOException("Missing checkpoint [" + file + "]");
            length = header.getInt(0);
            if (length > size) {
                throw new IOException("Checkpoint " + length +" exceeds file size " + size + " [" + file + "]");
            } else if (length < size) {
                channel.truncate(length);   // discard possibly corrupt portion
            }
            lastCheckpointLength = length;
        }
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    /**
     * Append a message and return its id (position in the file plus the baseOffset of the file itself).
     */
    public long append(long timestamp, String routingKey, ByteBuffer payload) throws IOException {
        if (routingKey.length() > 255) {
            throw new IllegalArgumentException("Routing key length " + routingKey.length() + " > 255 characters");
        }
        byte[] routingKeyBytes = routingKey.getBytes(UTF8);

        synchronized (channel) {
            header.clear();
            int id = (int)channel.size();
            if (id == 0) {
                header.putInt(0);   // new file so make space for checkpoint
                id += 4;
            } else {
                channel.position(id);
            }
            header.put(TYPE_MESSAGE);
            header.putLong(timestamp);
            header.putShort((short)routingKeyBytes.length);
            header.putInt(payload.limit());
            header.put(routingKeyBytes);
            header.flip();

            srcs[1] = payload;
            channel.write(srcs);
            length = (int)channel.position(); // update after write so a partial write won't corrupt file
            return baseOffset + id;
        }
    }

    /**
     * How big is this file in bytes?
     */
    public int length() throws IOException {
        synchronized (channel) {
            return length;
        }
    }

    /**
     * Sync all changes to disk and write a checkpoint to the file. Note that the checkpoint is not itself synced to
     * disk. If you want that call checkpoint twice.
     */
    public void checkpoint() throws IOException {
        synchronized (channel) {
            // force all writes to disk before updating checkpoint length so we know all data up to length is good
            channel.force(true);
            if (length != lastCheckpointLength) {
                header.clear();
                header.putInt(length);
                header.flip();
                channel.position(0).write(header);
                lastCheckpointLength = length;
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (channel) {
            checkpoint();
            raf.close();
        }
    }

    @Override
    public String toString() {
        return "MessageFile[" + file + "] baseOffset " + baseOffset + " length " + length;
    }

    /**
     * Create a cursor reading data from messageId onwards. To read the newest messages appearing in the file
     * do <code>mf.createCursor(mf.length())</code>. To read from the oldest message do
     * <code>mf.createCursor(mf.getBaseOffset())</code>.
     */
    public Cursor cursor(long messageId) throws IOException {
        messageId -= baseOffset;
        if (messageId < 0 || messageId > length) {
            throw new IllegalArgumentException("messageId " + (messageId + baseOffset) + " not in " + this);
        }
        int pos = (int)messageId;
        if (pos == 0) pos = 4;
        return new Cursor(pos);
    }

    /**
     * Iterates over messages in the file. Not thread safe.
     */
    public class Cursor {

        private final ChannelInput input;
        private final byte[] routingKeyBuf = new byte[1024];

        private long id;
        private long timestamp;
        private String routingKey;
        private int payloadSize;

        public Cursor(int position) {
            input = new ChannelInput(channel, position);
        }

        /**
         * Advance to the next message or return false if there are no more messages. The cursor initially starts
         * "before" the next message.
         */
        public boolean next() throws IOException {
            if (payloadSize > 0) input.skip(payloadSize);   // payload was never read

            int len = length();
            if (input.position() >= len) return false;

            id = baseOffset + input.position();

            byte type = input.readByte();
            if (type != TYPE_MESSAGE) {
                throw new IOException("Unexpected message type 0x" + Integer.toHexString(type & 0xFF) + " at " +
                        (input.position() - 1) + " in " + MessageFile.this);
            }

            timestamp = input.readLong();

            int routingKeySize = input.readShort();
            if (routingKeySize < 0 || routingKeySize >= routingKeyBuf.length) {
                throw new IOException("Invalid routing key size " + routingKeySize + " at " +
                        (input.position() - 2) + " in " + MessageFile.this);
            }

            payloadSize = input.readInt();
            if (payloadSize < 0) {
                throw new IOException("Negative payload size " + payloadSize + " at " + (input.position() - 4)  +
                        " in " + MessageFile.this);
            }
            if (input.position() + routingKeySize + payloadSize > len) {
                throw new IOException("Payload size " + payloadSize + " at " + (input.position() - 4) +
                        " extends beyond EOF " + len + " in " + MessageFile.this);
            }

            if (routingKeySize > 0) {
                input.read(routingKeyBuf, 0, routingKeySize);
                routingKey = new String(routingKeyBuf, 0, routingKeySize, UTF8);
            } else {
                routingKey = "";
            }

            return true;
        }

        public long getId() {
            return id;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getRoutingKey() {
            return routingKey;
        }

        public int getPayloadSize() {
            return payloadSize;
        }

        public byte[] getPayload() throws IOException {
            byte[] buf = new byte[payloadSize];
            input.read(buf, 0, payloadSize);
            payloadSize = 0;
            return buf;
        }
    }
}
