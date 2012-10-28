package io.qdb.buffer;

import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Stores messages on the file system in multiple files in a directory. Thread safe.
 */
public class FileMessageBuffer implements Closeable {

    private final File dir;

    private long maxBufferSize = Long.MAX_VALUE;
    private int maxFileSize = 100 * 1024 * 1024;    // 100 MB

    private long[] files;           // first message ID stored in each file (from filename)
    private int firstFile;          // index of first entry in files in use
    private int lastFile;           // index of last entry in files in use + 1

    private MessageFile current;    // file we are currently appending to
    private int lastFileLength;     // only used if current is null

    private static final FilenameFilter QDB_FILTER = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".qdb");
        }
    };

    public FileMessageBuffer(File dir) throws IOException {
        this(dir, 0L);
    }

    public FileMessageBuffer(File dir, long firstMessageId) throws IOException {
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                throw new IOException("Directory [" + dir + "] does not exist and could not be created");
            }
        }
        if (!dir.isDirectory()) {
            throw new IOException("Not a directory [" + dir + "]");
        }
        this.dir = dir;

        // build our master index from the names of the files in dir
        String[] list = dir.list(QDB_FILTER);
        if (list == null) {
            throw new IOException("Unable to list files in [" + dir + "]");
        }
        int n = list.length;
        files = new long[((n / 512) + 1) * 512];
        if (n > 0) {
            for (int i = 0; i < n; i++) {
                String name = list[i];
                try {
                    files[i] = Long.parseLong(name.substring(0, name.indexOf('.')), 16);
                } catch (NumberFormatException e) {
                    throw new IOException("File [" + dir + "/" + list[i] + "] has invalid name");
                }
            }
            lastFile = n;
            Arrays.sort(files, 0, n);
            lastFileLength = (int)getFile(lastFile - 1).length();
        } else {
            files[0] = firstMessageId;
        }
    }

    public long getMaxBufferSize() {
        return maxBufferSize;
    }

    /**
     * Set the maximum size of this buffer in bytes. When it is full the oldest messages are deleted to make space.
     */
    public void setMaxBufferSize(long maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    public int getMaxFileSize() {
        return maxFileSize;
    }

    /**
     * How big are the individual message files? Smaller files provide more granular histogram data but more files
     * are created on disk.
     */
    public void setMaxFileSize(int maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    /**
     * How much space is this buffer currently consuming in bytes?
     */
    public synchronized long getLength() {
        int c = lastFile - firstFile;
        if (c == 0) return 0L;
        return  (c - 1) * MessageFile.FILE_HEADER_SIZE +
                files[lastFile - 1] - files[firstFile] + (current == null ? lastFileLength : current.length());
    }

    /**
     * How many message files does this buffer have?
     */
    public synchronized int getFileCount() {
        return lastFile - firstFile;
    }

    /**
     * Append a message and return its id.
     */
    public synchronized long append(long timestamp, String routingKey, ByteBuffer payload) throws IOException {
        ensureCurrent();
        long id = current.append(timestamp, routingKey, payload);
        if (id < 0) {
            ensureSpaceInFiles();
            long firstMessageId = current.getNextMessageId();
            current.close();
            current = new MessageFile(toFile(firstMessageId), firstMessageId, maxFileSize);
            files[lastFile++] = firstMessageId;
            id = current.append(timestamp, routingKey, payload);
            if (id < 0) {
                throw new IllegalArgumentException("Message is too long, max size is approximately " + maxBufferSize +
                        " bytes");
            }
        }
        return id;
    }

    private void ensureCurrent() throws IOException {
        if (current == null) {
            if (lastFile == firstFile) {
                ensureSpaceInFiles();
                ++lastFile;
            }
            long firstMessageId = files[lastFile - 1];
            current = new MessageFile(toFile(firstMessageId), firstMessageId, maxFileSize);
        }
    }

    /**
     * Make sure there is space for one more entry in the files array.
     */
    private void ensureSpaceInFiles() {
        if (lastFile < files.length) return;
        int n = lastFile - firstFile;
        long[] a = new long[n + 512];
        System.arraycopy(files, firstFile, a, 0, n);
        files = a;
        lastFile -= firstFile;
        firstFile = 0;
    }

    private static final char[] ZERO_CHARS = "0000000000000000".toCharArray();

    private File toFile(long firstMessageId) {
        String name = Long.toHexString(firstMessageId);
        StringBuilder b = new StringBuilder();
        b.append(ZERO_CHARS, 0, ZERO_CHARS.length - name.length()).append(name).append(".qdb");
        return new File(dir, b.toString());
    }

    private File getFile(int i) {
        if (i < firstFile || i >= lastFile) {
            throw new IllegalArgumentException("Index " + i + " out of range (" + firstFile + " to " + (lastFile - 1) + ")");
        }
        return toFile(files[i]);
    }

    @Override
    public synchronized void close() throws IOException {
        if (current != null) current.close();
    }

    /**
     * What ID will the next message appended have?
     */
    public synchronized long getNextMessageId() throws IOException {
        ensureCurrent();
        return current.getNextMessageId();
    }

    @Override
    public String toString() {
        return "FileMessageBuffer[" + dir + "]";
    }

    /**
     * Create a cursor reading data from messageId onwards. To read the oldest message use 0 as the message ID. To
     * read the newest use {@link #getNextMessageId()}. If the messageId is before the oldest message the the cursor
     * reads from the oldest message onwards.
     */
    public synchronized MessageCursor cursor(long messageId) throws IOException {
        if (messageId < 0) {
            throw new IllegalArgumentException("Invalid messageId " + messageId + ", " + this);
        }
        long next = getNextMessageId();
        if (messageId > next) {
            throw new IllegalArgumentException("messageId " + messageId + " past end of buffer " + next + ", " + this);
        }
        int i = findFileIndex(messageId);
        MessageFile mf = i >= lastFile ? current : new MessageFile(getFile(i), files[i]);
        return null;
    }

    /**
     * Get the index of the file containing messageId.
     */
    private int findFileIndex(long messageId) {
        if (messageId == 0) return firstFile;
        int i = Arrays.binarySearch(files, firstFile, lastFile, messageId);
        return i >= 0 ? i  : -(i + 1);
    }

    public class Cursor implements MessageCursor {

        @Override
        public boolean next() throws IOException {
            return false;
        }

        @Override
        public long getId() throws IOException {
            return 0;
        }

        @Override
        public long getTimestamp() throws IOException {
            return 0;
        }

        @Override
        public String getRoutingKey() throws IOException {
            return null;
        }

        @Override
        public int getPayloadSize() throws IOException {
            return 0;
        }

        @Override
        public byte[] getPayload() throws IOException {
            return new byte[0];
        }

        @Override
        public void close() throws IOException {
        }
    }

}
