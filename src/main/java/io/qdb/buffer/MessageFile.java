package io.qdb.buffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

/**
 * <p>A bunch of messages all in the same file. Supports fast seek to a message by timestamp and detection and
 * recovery from corruption due to server crash. New messages are always appended to the end of the file for
 * performance. Thread safe.</p>
 *
 * <p>The file header is 4096 bytes long. The fixed part is 16 bytes and has the following format
 * (all BIG_ENDIAN):</p>
 * <pre>
 * magic: 2 bytes (currently 0xBE01)
 * reserved: 2 bytes (currently 0x0000)
 * max file size: 4 bytes
 * length of file at last checkpoint: 4 bytes
 * reserved: 4 bytes (currently 0x00000000)
 * </pre>
 *
 * <p>Recovery from a crash is simply a matter of truncating the file to its last checkpoint length. That might
 * discard some good messages but has the advantage of being very fast (compared to calculating and checking
 * message CRC values for example). The assumption is that if the messages are very important they will be
 * written to multiple machines.</p>
 *
 * <p>The rest of the file header consists of up to 340 histogram buckets for fast message lookup by timestamp:</p>
 * <pre>
 * first message id (relative to this file): 4 bytes
 * timestamp in unix time: 4 bytes
 * message count: 4 bytes
 * </pre>
 *
 * <p>The histogram is updated at each checkpoint. Checkpoints are done manually or automatically every max file
 * size / 340 bytes.</p>
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
    private final long firstMessageId;
    private final int maxFileSize;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final ByteBuffer fileHeader;
    private final ByteBuffer header;
    private final ByteBuffer[] srcs = new ByteBuffer[2];

    private int length;
    private int lastCheckpointLength;

    private final int bytesPerBucket;
    private int bucketIndex;
    private int bucketTime;
    private int bucketMessageId;
    private int bucketCount;

    public static final int FILE_HEADER_SIZE = 4096;
    private static final int FILE_HEADER_FIXED_SIZE = 16;
    private static final int BUCKET_RECORD_SIZE = 12;
    private static final int MAX_BUCKETS = (FILE_HEADER_SIZE - FILE_HEADER_FIXED_SIZE) / BUCKET_RECORD_SIZE;

    private static final short FILE_MAGIC = (short)0xBE01;

    private static final byte TYPE_MESSAGE = (byte)0xA1;

    private static final int MESSAGE_HEADER_SIZE = 1 + 8 + 2 + 4;

    private static final Charset UTF8 = Charset.forName("UTF8");

    /**
     * Open an existing file.
     */
    @SuppressWarnings("StatementWithEmptyBody")
    public MessageFile(File file, long firstMessageId) throws IOException {
        this(file, firstMessageId, -1);
    }

    /**
     * Open a new or existing file. The maxFileSize parameter is only used when creating a new file.
     */
    @SuppressWarnings("StatementWithEmptyBody")
    public MessageFile(File file, long firstMessageId, int maxFileSize) throws IOException {
        this.file = file;
        this.firstMessageId = firstMessageId;

        if (maxFileSize < 0 && !file.isFile()) {
            throw new IllegalArgumentException("File does not exist, is not readable or is not a file [" + file + "]");
        }

        raf = new RandomAccessFile(file, "rw");
        channel = raf.getChannel();
        fileHeader = ByteBuffer.allocateDirect(FILE_HEADER_SIZE);
        header = ByteBuffer.allocateDirect(1024);
        srcs[0] = header;

        int size = (int)channel.size();
        if (size == 0) {
            if (maxFileSize < FILE_HEADER_SIZE) {
                throw new IllegalArgumentException("Invalid max file size " + maxFileSize);
            }
            fileHeader.putShort(FILE_MAGIC);
            fileHeader.putShort((short)0);
            fileHeader.putInt(this.maxFileSize = maxFileSize);
            fileHeader.putInt(length = FILE_HEADER_SIZE);
            fileHeader.putInt(0);
            channel.write(fileHeader);
            bucketIndex = -1;
        } else {
            int sz = channel.read(fileHeader);
            if (sz < FILE_HEADER_SIZE) throw new IOException("File header too short [" + file + "]");
            fileHeader.flip();
            if (fileHeader.getShort() != FILE_MAGIC) throw new IOException("Invalid file magic [" + file + "]");
            fileHeader.position(fileHeader.position() + 2);
            this.maxFileSize = fileHeader.getInt();
            if (this.maxFileSize < FILE_HEADER_SIZE) {
                throw new IOException("Invalid max file size " + this.maxFileSize + " [" + file + "]");
            }
            length = fileHeader.getInt();
            if (length > size) {
                throw new IOException("Checkpoint " + length + " exceeds file size " + size + " [" + file + "]");
            } else if (length < size) {
                channel.truncate(length);   // discard possibly corrupt portion
            }
            lastCheckpointLength = length;

            for (bucketIndex = 1; bucketIndex < MAX_BUCKETS && fileHeader.getInt(bucketPosition(bucketIndex++)) != 0; );

            fileHeader.position(bucketPosition(--bucketIndex));
            bucketMessageId = fileHeader.getInt();
            bucketTime = fileHeader.getInt();
            bucketCount = fileHeader.getInt();
        }

        bytesPerBucket = (maxFileSize - FILE_HEADER_SIZE) / MAX_BUCKETS;
    }

    private int bucketPosition(int i) {
        return FILE_HEADER_FIXED_SIZE + i * BUCKET_RECORD_SIZE;
    }

    public long getFirstMessageId() {
        return firstMessageId;
    }

    /**
     * What ID will the next message appended have, assuming there is space for it?
     */
    public long getNextMessageId() {
        return firstMessageId + length - FILE_HEADER_SIZE;
    }

    /**
     * Append a message and return its id (position in the file plus the firstMessageId of the file). Returns
     * -1 if this file is too full for the message.
     */
    public long append(long timestamp, String routingKey, ByteBuffer payload) throws IOException {
        int n = routingKey.length();
        if (n > 255) throw new IllegalArgumentException("Routing key length " + n + " > 255 characters");

        byte[] routingKeyBytes = routingKey.getBytes(UTF8);

        synchronized (channel) {
            if (length + MESSAGE_HEADER_SIZE + routingKeyBytes.length + payload.limit() > maxFileSize) return -1;

            header.clear();
            channel.position(length);
            header.put(TYPE_MESSAGE);
            header.putLong(timestamp);
            header.putShort((short)routingKeyBytes.length);
            header.putInt(payload.limit());
            header.put(routingKeyBytes);
            header.flip();

            srcs[1] = payload;
            channel.write(srcs);

            int id = length - FILE_HEADER_SIZE;
            length = (int)channel.position(); // update after write so a partial write won't corrupt file

            // see if we need to start a new histogram bucket
            if (bucketIndex < 0 || ((id - bucketMessageId >= bytesPerBucket) && bucketIndex < MAX_BUCKETS - 1)) {
                if (bucketIndex >= 0) {
                    putBucketDataInFileHeader();
                    ++bucketIndex;
                } else {
                    bucketIndex = 0;
                }
                bucketMessageId = id;
                bucketTime = (int)(timestamp / 1000);
                bucketCount = 1;
            } else {
                ++bucketCount;
            }

            return firstMessageId + id;
        }
    }

    private void putBucketDataInFileHeader() {
        fileHeader.position(bucketPosition(bucketIndex));
        fileHeader.putInt(bucketMessageId);
        fileHeader.putInt(bucketTime);
        fileHeader.putInt(bucketCount);
        // data will be written at the next checkpoint
    }

    /**
     * How big is this file in bytes?
     */
    public int length() {
        synchronized (channel) {
            return length;
        }
    }

    /**
     * Sync all changes to disk and write a checkpoint to the file. Note that the checkpoint is itself synced to
     * disk only if force is true.
     */
    public void checkpoint(boolean force) throws IOException {
        synchronized (channel) {
            // force all writes to disk before updating checkpoint length so we know all data up to length is good
            channel.force(true);
            if (length != lastCheckpointLength) {
                fileHeader.putInt(8, length);
                if (bucketIndex >= 0) putBucketDataInFileHeader();
                fileHeader.position(0);
                channel.position(0).write(fileHeader);
                lastCheckpointLength = length;
                if (force) channel.force(true);
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (channel) {
            checkpoint(true);
            raf.close();
        }
    }

    @Override
    public String toString() {
        return "MessageFile[" + file + "] firstMessageId " + firstMessageId + " length " + length;
    }

    /**
     * How many histogram buckets are there?
     */
    public int getBucketCount() {
        synchronized (channel) {
            return bucketIndex + 1;
        }
    }

    /**
     * Get a copy of the data for the histogram bucket at index.
     */
    public Bucket getBucket(int i) {
        synchronized (channel) {
            if (i < 0 || i > bucketIndex) {
                throw new IllegalArgumentException("index " + i + " out of range (0 to " + bucketIndex + ")");
            }
            if (i == bucketIndex) {
                return new Bucket(firstMessageId + bucketMessageId, bucketTime, bucketCount,
                        (length - FILE_HEADER_SIZE) - bucketMessageId);
            }
            fileHeader.position(bucketPosition(i));
            int id = fileHeader.getInt();
            return new Bucket(firstMessageId + id, fileHeader.getInt(), fileHeader.getInt(),
                    (i == bucketIndex - 1 ? bucketMessageId : fileHeader.getInt()) - id);
        }
    }

    /**
     * Get the index of the histogram bucket containing messageId or -1 if it is before the first message id or this
     * file is empty. If messageId is after the last the last bucket index is returned.
     */
    public int findBucketIndex(long messageId) throws IOException {
        synchronized (channel) {
            int key = (int)(messageId - firstMessageId);
            if (key >= bucketMessageId) return bucketIndex; // last bucket
            int low = 0;
            int high = bucketIndex - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                int midVal = fileHeader.getInt(bucketPosition(mid));
                if (midVal < key) low = mid + 1;
                else if (midVal > key) high = mid - 1;
                else return mid;
            }
            return low - 1;
        }
    }

    public static class Bucket {

        private final long firstMessageId;
        private final int time;
        private final int count;
        private final int size;

        public Bucket(long firstMessageId, int time, int count, int size) {
            this.firstMessageId = firstMessageId;
            this.time = time;
            this.count = count;
            this.size = size;
        }

        /**
         * Get the unix time of the first message in the bucket. This is the timestamp of the message / 1000.
         */
        public int getTime() {
            return time;
        }

        /**
         * Get the ID of the first message in the bucket.
         */
        public long getFirstMessageId() {
            return firstMessageId;
        }

        /**
         * Get the number of messages in the bucket.
         */
        public int getCount() {
            return count;
        }

        /**
         * Get the number of bytes of messages in the bucket.
         */
        public int getSize() {
            return size;
        }
    }

    /**
     * Create a cursor reading data from messageId onwards. To read the oldest message appearing in the file
     * use {@link #getFirstMessageId()} as the message ID. To read the newest use {@link #getNextMessageId()}.
     */
    public MessageCursor cursor(long messageId) throws IOException {
        if (messageId < firstMessageId || messageId > getNextMessageId()) {
            throw new IllegalArgumentException("messageId " + (messageId + firstMessageId) + " not in " + this);
        }
        return new Cursor((int)(messageId - firstMessageId) + FILE_HEADER_SIZE);
    }

    /**
     * Iterates over messages in the file. Not thread safe.
     */
    public class Cursor implements MessageCursor {

        private final ChannelInput input;
        private final byte[] routingKeyBuf = new byte[1024];

        private long id;
        private long timestamp;
        private String routingKey;
        private int payloadSize;

        public Cursor(int position) {
            input = new ChannelInput(channel, position, 8192);
        }

        /**
         * Advance to the next message or return false if there are no more messages. The cursor initially starts
         * "before" the next message.
         */
        public boolean next() throws IOException {
            if (payloadSize > 0) input.skip(payloadSize);   // payload was never read

            int len = length();
            if (input.position() >= len) return false;

            id = firstMessageId + input.position() - FILE_HEADER_SIZE;

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
