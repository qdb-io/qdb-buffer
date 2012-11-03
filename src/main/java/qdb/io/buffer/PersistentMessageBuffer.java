package qdb.io.buffer;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.Executor;

/**
 * <p>Stores messages on the file system in multiple files in a directory. Thread safe. The files are named
 * [id of first message in hex, 16 digits]-[timestamp of first message in hex, 16 digits].qdb so that they sort
 * in message id order.</p>
 */
public class PersistentMessageBuffer implements MessageBuffer {

    private final File dir;

    private long maxLength;
    private int maxFileSize = 100 * 1024 * 1024;    // 100 MB

    private long[] files;           // first message ID stored in each file (from filename)
    private long[] timestamps;      // timestamp of first message stored in each file (from filename)
    private int firstFile;          // index of first entry in files in use
    private int lastFile;           // index of last entry in files in use + 1

    private MessageFile current;    // file we are currently appending to
    private int lastFileLength;     // only used if current is null

    private Executor cleanupExecutor;
    private Runnable cleanupJob;

    private static final FilenameFilter QDB_FILTER = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".qdb");
        }
    };

    public PersistentMessageBuffer(File dir) throws IOException {
        this(dir, 0L);
    }

    public PersistentMessageBuffer(File dir, long firstMessageId) throws IOException {
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
        Arrays.sort(list);
        int n = list.length;
        int len = ((n / 512) + 1) * 512;
        files = new long[len];
        timestamps = new long[len];
        if (n > 0) {
            for (int i = 0; i < n; i++) {
                String name = list[i];
                if (name.length() != 37) {
                    throw new IOException("File [" + dir + "/" + list[i] + "] has invalid name");
                }
                try {
                    files[i] = Long.parseLong(name.substring(0, 16), 16);
                    timestamps[i] = Long.parseLong(name.substring(17, 33), 16);
                } catch (NumberFormatException e) {
                    throw new IOException("File [" + dir + "/" + list[i] + "] has invalid name");
                }
            }
            lastFile = n;
            lastFileLength = (int)getFile(lastFile - 1).length();
        } else {
            files[0] = firstMessageId;
        }
    }

    @Override
    public void setMaxLength(long bytes) throws IOException {
        this.maxLength = bytes;
        cleanup();
    }

    @Override
    public long getMaxLength() {
        return maxLength;
    }

    public int getMaxFileSize() {
        return maxFileSize;
    }

    /**
     * Provide an executor (e.g. thread pool) to do cleanup's asynchronously when the buffer starts a new message
     * file. If no executor is set then cleanups are done synchronously i.e. on the thread appending the message.
     */
    public void setCleanupExecutor(Executor cleanupExecutor) {
        this.cleanupExecutor = cleanupExecutor;
        if (cleanupJob == null) {
            cleanupJob = new Runnable() {
                @Override
                public void run() {
                    try {
                        cleanup();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }
    }

    public Executor getCleanupExecutor() {
        return cleanupExecutor;
    }

    /**
     * How big are the individual message files? Smaller files provide more granular histogram data but more files
     * are created on disk.
     */
    public void setMaxFileSize(int maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    @Override
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

    @Override
    public synchronized long append(long timestamp, String routingKey, ByteBuffer payload) throws IOException {
        if (current == null) {
            if (lastFile == 0) {    // new buffer
                current = new MessageFile(toFile(files[0], timestamps[0] = timestamp), files[0], maxFileSize);
                ++lastFile;
            } else {
                ensureCurrent();
            }
        }
        long id = current.append(timestamp, routingKey, payload);
        if (id < 0) {
            ensureSpaceInFiles();
            long firstMessageId = current.getNextMessageId();
            current.close();
            current = new MessageFile(toFile(firstMessageId, timestamp), firstMessageId, maxFileSize);
            timestamps[lastFile] = timestamp;
            files[lastFile++] = firstMessageId;
            id = current.append(timestamp, routingKey, payload);
            if (id < 0) {
                throw new IllegalArgumentException("Message is too long, max size is approximately " + maxFileSize +
                        " bytes");
            }
            if (cleanupExecutor != null) {
                cleanupExecutor.execute(cleanupJob);
            } else {
                cleanup();
            }
        }
        return id;
    }

    private void ensureCurrent() throws IOException {
        if (current == null) {
            long firstMessageId = files[lastFile - 1];
            current = new MessageFile(toFile(firstMessageId, timestamps[lastFile - 1]), firstMessageId);
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

        a = new long[n + 512];
        System.arraycopy(timestamps, firstFile, a, 0, n);
        timestamps = a;

        lastFile -= firstFile;
        firstFile = 0;
    }

    /**
     * If this buffer is exceeding its maximum capacity then delete some of the the oldest files until it is under
     * the limit.
     */
    public void cleanup() throws IOException {
        for (;;) {
            File doomed;
            synchronized (this) {
                if (maxLength == 0 || getLength() <= maxLength || firstFile >= lastFile - 1) return;
                doomed = getFile(firstFile);
                ++firstFile;
                // todo what about cursors that might have doomed open?
            }
            if (!doomed.delete()) {
                throw new IOException("Unable to delete [" + doomed + "]");
            }
        }
    }

    @Override
    public synchronized void sync() throws IOException {
        if (current != null) {
            current.checkpoint(true);
        }
    }

    private static final char[] ZERO_CHARS = "0000000000000000".toCharArray();

    private File toFile(long firstMessageId, long timestamp) {
        StringBuilder b = new StringBuilder();
        String name = Long.toHexString(firstMessageId);
        b.append(ZERO_CHARS, 0, ZERO_CHARS.length - name.length()).append(name).append('-');
        name = Long.toHexString(timestamp);
        b.append(ZERO_CHARS, 0, ZERO_CHARS.length - name.length()).append(name).append(".qdb");
        return new File(dir, b.toString());
    }

    private File getFile(int i) {
        if (i < firstFile || i >= lastFile) {
            throw new IllegalArgumentException("Index " + i + " out of range (" + firstFile + " to " + (lastFile - 1) + ")");
        }
        return toFile(files[i], timestamps[i]);
    }

    @Override
    public synchronized void close() throws IOException {
        if (current != null) current.close();
    }

    @Override
    public synchronized long getNextMessageId() throws IOException {
        if (lastFile == 0) {
            return files[lastFile];  // empty buffer
        }
        ensureCurrent();
        return current.getNextMessageId();
    }

    @Override
    public synchronized Timeline getTimeline() throws IOException {
        int n = lastFile - firstFile;
        if (n == 0) return null;    // buffer is empty
        TopTimeline ans = new TopTimeline(n + 1);
        System.arraycopy(this.files, firstFile, ans.files, 0, n);
        System.arraycopy(this.timestamps, firstFile, ans.timestamps, 0, n);
        ensureCurrent();
        ans.files[n] = current.getNextMessageId();
        long mrt = current.getMostRecentTimestamp();
        ans.timestamps[n] = mrt == 0 ? ans.timestamps[n - 1] : mrt;
        return ans;
    }

    static class TopTimeline implements Timeline {

        private long[] files, timestamps;

        TopTimeline(int n) {
            files = new long[n];
            timestamps = new long[n];
        }

        public int size() {
            return files.length;
        }

        public long getMessageId(int i) {
            return files[i];
        }

        public long getTimestamp(int i) {
            return timestamps[i];
        }

        public int getBytes(int i) {
            return i == files.length - 1 ? 0 : (int)(files[i + 1] - files[i]);
        }

        public long getMillis(int i) {
            return i == files.length - 1 ? 0L : timestamps[i + 1] - timestamps[i];
        }

        public long getCount(int i) {
            return 0;
        }
    }

    @Override
    public Timeline getTimeline(long messageId) throws IOException {
        return null;
    }

    @Override
    public String toString() {
        return "FileMessageBuffer[" + dir + "]";
    }

    @Override
    public MessageCursor cursor(long messageId) throws IOException {
        if (messageId < 0) {
            throw new IllegalArgumentException("Invalid messageId " + messageId + ", " + this);
        }
        long next = getNextMessageId();
        if (messageId > next) {
            throw new IllegalArgumentException("messageId " + messageId + " past end of buffer " + next + ", " + this);
        }

        int i;
        synchronized (this) {
            if (lastFile == firstFile) {
                return new EmptyCursor();
            }
            long firstMessageId = files[firstFile];
            if (messageId < firstMessageId) {
                messageId = firstMessageId;
            }

            i = Arrays.binarySearch(files, firstFile, lastFile, messageId);
            if (i < 0) {
                i = -(i + 2); // return position before the insertion index if we didn't get a match
            }
        }

        MessageFile mf = getMessageFileForCursor(i);
        return new Cursor(i, mf, mf.cursor(messageId));
    }

    @Override
    public MessageCursor cursorByTimestamp(long timestamp) throws IOException {
        int i;
        synchronized (this) {
            if (lastFile == firstFile) {
                return new EmptyCursor();
            }
            long firstTimestamp = timestamps[firstFile];
            if (timestamp < firstTimestamp) {
                timestamp = firstTimestamp;
            }

            i = Arrays.binarySearch(timestamps, firstFile, lastFile, timestamp);
            if (i < 0) {
                i = -(i + 2); // return position before the insertion index if we didn't get a match
            }
        }

        MessageFile mf = getMessageFileForCursor(i);
        return new Cursor(i, mf, mf.cursorByTimestamp(timestamp));
    }

    private MessageFile getMessageFileForCursor(int i) throws IOException {
        synchronized (this) {
            if (i == lastFile - 1) {
                current.use();
                return current;
            } else if (i >= lastFile) {
                return null;
            }
        }
        return new MessageFile(getFile(i), files[i]);
    }

    private class Cursor implements MessageCursor {

        protected int fileIndex;
        protected MessageFile mf;
        protected MessageCursor c;

        public Cursor(int fileIndex, MessageFile mf, MessageCursor c) {
            this.fileIndex = fileIndex;
            this.mf = mf;
            this.c = c;
        }

        public boolean next() throws IOException {
            if (c.next()) return true;
            synchronized (this) {
                if (mf == current) return false;
            }
            close();
            mf = getMessageFileForCursor(++fileIndex);
            c = mf.cursor(mf.getFirstMessageId());
            return c.next();
        }

        public long getId() throws IOException {
            return c.getId();
        }

        public long getTimestamp() throws IOException {
            return c.getTimestamp();
        }

        public String getRoutingKey() throws IOException {
            return c.getRoutingKey();
        }

        public int getPayloadSize() throws IOException {
            return c.getPayloadSize();
        }

        public byte[] getPayload() throws IOException {
            return c.getPayload();
        }

        public void close() throws IOException {
            if (c != null) {
                c.close();
                c = null;
            }
            if (mf != null) {
                mf.close();
                mf = null;
            }
        }

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            close();
        }
    }

    /**
     * This implementation is used when the buffer is empty. It checks to see if the buffer is still empty on
     * each call to next and initializes the cursor when the buffer becomes not empty.
     */
    private class EmptyCursor extends Cursor {

        private EmptyCursor() {
            super(-1, null, null);
        }

        @Override
        public boolean next() throws IOException {
            if (fileIndex < 0) {
                mf = getMessageFileForCursor(0);
                if (mf == null) return false;
                fileIndex = 0;
                c = mf.cursor(mf.getFirstMessageId());
            }
            return super.next();
        }
    }

}
