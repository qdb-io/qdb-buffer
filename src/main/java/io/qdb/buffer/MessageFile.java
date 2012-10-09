package io.qdb.buffer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * <p>A bunch of messages all in the same file. Supports detection and recovery from corruption due to server crash.
 * New messages are always appended to the end of the file.<p>
 *
 * <p>The first 4 bytes of the file hold its length at the last checkpoint. Recovery from a crash is simply a matter
 * of truncating the file to its last checkpoint length. That might discard some good messages but has the advantage
 * of being very fast (compared to calculating and checking message CRC values for example). The assumption is that
 * if the messages are very important they will be written to separate machines.</p>
 *
 * <p>The remainder of the file consists of records in the following format (all BIG_ENDIAN):</p>
 *
 * <pre>
 * length: 4 bytes (value: 4 + 1 + 8 + 1 + m + n)
 * record type: 1 byte (value always 0xA1 currently)
 * timestamp: 8 bytes
 * routing key length: 1 byte
 * routing key UTF8 encoded: m bytes
 * payload: n bytes
 * </pre>
 */
class MessageFile implements Closeable {

    private final File file;
    private final long baseOffset;
    private RandomAccessFile raf;
    private FileChannel channel;
    private ByteBuffer header;
    private final ByteBuffer[] srcs = new ByteBuffer[2];

    private static final int HEADER_SIZE = 4 + 1 + 8 + 1;

    private static final byte TYPE_MESSAGE = (byte)0xA1;

    public MessageFile(File file, long baseOffset) throws IOException {
        this.file = file;
        this.baseOffset = baseOffset;
        raf = new RandomAccessFile(file, "rw");
        channel = raf.getChannel();
        header = ByteBuffer.allocateDirect(512);
        srcs[0] = header;

        if (channel.size() != 0L) {
        }
    }

    /**
     * Append a message and return its id (position in the file plus the baseOffset of the file itself).
     */
    public synchronized long append(long timestamp, String routingKey, ByteBuffer payload) throws IOException {
        byte[] routingKeyBytes = routingKey.getBytes("UTF8");
        int m = routingKeyBytes.length;
        if (m > 255) throw new IllegalArgumentException("Routing key length " + m + " > 255 bytes");
        int length = HEADER_SIZE + m + payload.limit();

        header.clear();
        int id = (int)channel.size();
        if (id == 0) {
            header.putInt(0);   // new file so make space for checkpoint
            id += 4;
        } else {
            channel.position(id);
        }
        header.putInt(length);
        header.put(TYPE_MESSAGE);
        header.putLong(timestamp);
        header.put((byte) (m & 0xFF));
        header.put(routingKeyBytes);
        header.flip();

        srcs[1] = payload;
        channel.write(srcs);
        return baseOffset + id;
    }

    @Override
    public void close() throws IOException {
        raf.close();
    }

    @Override
    public String toString() {
        return "MessageFile[" + file + "]";
    }
}
