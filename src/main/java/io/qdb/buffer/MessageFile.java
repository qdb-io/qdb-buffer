package io.qdb.buffer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

/**
 * <p>A bunch of messages all in the same file. Supports detection and recovery from corruption due to server crash
 * and so on. New messages are always appended to the end of the file.<p>
 *
 * <p>Uses a FileChannel with MappedByteBuffer.
 * http://nadeausoftware.com/articles/2008/02/java_tip_how_read_files_quickly</p>
 *
 * <p>File format:</p>
 * <pre>
 * length: 4 bytes (value: 4 + 1 + 8 + 4 + n)
 * version: 1 byte
 * timestamp: 8 bytes
 * crc: 4 bytes
 * payload: n bytes
 * </pre>
 */
class MessageFile implements Closeable {

    private final File file;
    private RandomAccessFile raf;
    private FileChannel channel;
    private ByteBuffer headerBuffer;
    private final ByteBuffer[] srcs = new ByteBuffer[2];

    private static final byte VERSION = (byte)0x42;
    private static final int HEADER_SIZE = 4 + 1 + 8 + 4;

    public MessageFile(File file) throws IOException {
        this.file = file;
        raf = new RandomAccessFile(file, "rw");
        channel = raf.getChannel();
        headerBuffer = ByteBuffer.allocateDirect(HEADER_SIZE);
        srcs[0] = headerBuffer;
    }

    /**
     * Append a message and return its position in the file.
     */
    public long append(byte[] buffer, int offset, int length) throws IOException {
        long id = channel.size();
        headerBuffer.position(0);
        headerBuffer.putInt(length + HEADER_SIZE);
        headerBuffer.put(VERSION);
        headerBuffer.putLong(System.currentTimeMillis());
        CRC32 crc32 = new CRC32();
        crc32.update(buffer, offset, length);
        headerBuffer.putInt((int)crc32.getValue());
        headerBuffer.position(0);
        srcs[1] = ByteBuffer.wrap(buffer, offset, length);
        channel.position(id);
        channel.write(srcs);
        return id;
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
