package io.qdb.buffer;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;

public class FileMessageBufferTest {

    private static File dir = new File("build/test-data");

    @BeforeClass
    public static void beforeClass() throws IOException {
        if (!dir.isDirectory() && !dir.mkdirs()) {
            throw new IOException("Unable to create [" + dir + "]");
        }
    }

    @Test
    public void testAppend() throws IOException {
        FileMessageBuffer b = new FileMessageBuffer(mkdir("append"));
        b.setMaxFileSize(10000 + MessageFile.FILE_HEADER_SIZE);
        assertEquals(0, b.getFileCount());
        assertEquals(0L, b.getLength());
        assertEquals(10000 + MessageFile.FILE_HEADER_SIZE, b.getMaxFileSize());

        long ts = System.currentTimeMillis();
        assertEquals(0L, append(b, ts, "", 5000));
        assertEquals(5000L, append(b, ts, "", 5000));
        assertEquals(1, b.getFileCount());
        assertEquals(10000L + MessageFile.FILE_HEADER_SIZE, b.getLength());

        assertEquals(10000L, append(b, ts, "", 5000));
        assertEquals(2, b.getFileCount());
        assertEquals(15000L + MessageFile.FILE_HEADER_SIZE * 2, b.getLength());

        assertEquals(15000L, append(b, ts, "", 5000));
        assertEquals(2, b.getFileCount());
        assertEquals(20000L + MessageFile.FILE_HEADER_SIZE * 2, b.getLength());

        b.close();
    }

    @Test
    public void testFirstMessageId() throws IOException {
        File bd = mkdir("firstmsg");

        FileMessageBuffer b = new FileMessageBuffer(bd, 0x1234);
        long ts = 0x5678;
        assertEquals(0x1234L, append(b, ts, "", 256));
        b.close();

        expect(bd.list(), "0000000000001234-0000000000005678.qdb");

        b = new FileMessageBuffer(bd);
        assertEquals(0x1334L, append(b, ts, "", 256));
        b.close();
    }

    @Test
    public void testOpenExisting() throws IOException {
        File bd = mkdir("open-existing");

        FileMessageBuffer b = new FileMessageBuffer(bd);
        b.setMaxFileSize(8192 + MessageFile.FILE_HEADER_SIZE);
        long ts = 0x5678;
        append(b, ts, "", 4096);
        append(b, ts, "", 4096);
        b.close();

        expect(bd.list(), "0000000000000000-0000000000005678.qdb");

        b = new FileMessageBuffer(bd);
        b.setMaxFileSize(8192 + MessageFile.FILE_HEADER_SIZE);
        ts = 0x9abc;
        append(b, ts, "", 4096);
        b.close();

        expect(bd.list(), "0000000000000000-0000000000005678.qdb", "0000000000002000-0000000000009abc.qdb");
    }

    @Test
    public void testNextMessageId() throws IOException {
        File bd = mkdir("nextmsg");

        FileMessageBuffer b = new FileMessageBuffer(bd, 0x1234);
        assertEquals(0x1234L, b.getNextMessageId());

        long ts = System.currentTimeMillis();
        append(b, ts, "", 256);
        assertEquals(0x1334L, b.getNextMessageId());
        b.close();

        b = new FileMessageBuffer(bd);
        assertEquals(0x1334L, b.getNextMessageId());
        b.close();
    }

    @Test
    public void testMoreThan512Files() throws IOException {
        File bd = mkdir("files512");

        FileMessageBuffer b = new FileMessageBuffer(bd);
        b.setMaxFileSize(8192 + MessageFile.FILE_HEADER_SIZE);
        int ts = 0;
        int n = 513;
        String[] expect = new String[n];
        for (int i = 0; i < n; i++) {
            append(b, ++ts, "", 8192);
            expect[i] = "00000000" + String.format("%08x", i * 8192) + "-00000000" + String.format("%08x", ts) + ".qdb";
        }
        b.close();

        expect(bd.list(), expect);
    }

    @Test
    public void testCursor() throws IOException {
        File bd = mkdir("cursor");
        Random rnd = new Random(123);

        FileMessageBuffer b = new FileMessageBuffer(bd);
        b.setMaxFileSize(8192 + MessageFile.FILE_HEADER_SIZE);

        MessageCursor c = b.cursor(0);
        assertFalse(c.next());

        Msg m0 = appendFixedSizeMsg(b, 100, 4096, rnd);
        assertNextMsg(m0, c);
        assertFalse(c.next());
        c.close();

        // cursor starting on an empty buffer is a special case so repeat the test with a 'normal' cursor
        c = b.cursor(0);
        assertNextMsg(m0, c);
        assertFalse(c.next());

        // this fills up the first file
        Msg m1 = appendFixedSizeMsg(b, 100, 4096, rnd);
        assertNextMsg(m1, c);
        assertFalse(c.next());

        // file the 2nd file and start the 3rd
        Msg m2 = appendFixedSizeMsg(b, 100, 4096, rnd);
        Msg m3 = appendFixedSizeMsg(b, 100, 4096, rnd);
        Msg m4 = appendFixedSizeMsg(b, 100, 4096, rnd);

        // these messages are fetched from 2nd file (not current file)
        assertNextMsg(m2, c);
        assertNextMsg(m3, c);

        // this one comes from current
        assertNextMsg(m4, c);
        assertFalse(c.next());

        b.close();
    }

    private void assertNextMsg(Msg msg, MessageCursor c) throws IOException {
        assertTrue(c.next());
        assertEquals(msg.id, c.getId());
        assertEquals(msg.timestamp, c.getTimestamp());
        assertEquals(msg.routingKey, c.getRoutingKey());
        assertEquals(msg.payload.length, c.getPayloadSize());
        assertArrayEquals(msg.payload, c.getPayload());
    }

    private Msg appendFixedSizeMsg(FileMessageBuffer b, long ts, int totalSize, Random rnd) throws IOException {
        String key = "key" + ts;
        byte[] payload = new byte[totalSize - 15 - key.length()];
        rnd.nextBytes(payload);
        Msg msg = new Msg(ts, key, payload);
        msg.id = b.append(msg.timestamp, msg.routingKey, ByteBuffer.wrap(msg.payload));
        return msg;
    }

    private void expect(String[] actual, String... expected) {
        Arrays.sort(actual);
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals("[" + i + "]", expected[i], actual[i]);
        }
    }

    private long append(FileMessageBuffer b, long timestamp, String key, int len) throws IOException {
        byte[] payload = new byte[len - 15 - key.length()];
        return b.append(timestamp, key, ByteBuffer.wrap(payload));
    }

    @SuppressWarnings("ConstantConditions")
    private File mkdir(String name) throws IOException {
        File f = new File(dir, name);
        if (f.isDirectory()) {
            for (File file : f.listFiles()) {
                if (!file.delete()) throw new IOException("Unable to delete [" + file + "]");
            }
        }
        return f;
    }

}