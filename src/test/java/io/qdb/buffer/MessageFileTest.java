package io.qdb.buffer;

import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Random;

import static junit.framework.Assert.*;
import static org.junit.Assert.assertArrayEquals;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class MessageFileTest {

    private static File dir = new File("build/test-data");

    @Test
    public void testAppend() throws IOException {
        File file = new File(dir, "append.qdb");
        file.delete();

        MessageFile mf = new MessageFile(file, 1000L, 1000000);
        assertEquals(4096, mf.length());

        assertEquals(1000L, mf.getFirstMessageId());
        assertEquals(1000L, mf.getNextMessageId());

        long ts0 = System.currentTimeMillis();
        String key0 = "foo";
        byte[] payload0 = "piggy".getBytes("UTF8");
        int length0 = 1/*type*/ + 8/*timestamp*/ + 2/*key size*/ + 4/* payload size*/ + key0.length() + payload0.length;

        long ts1 = ts0 + 1;
        String key1 = "foobar";
        byte[] payload1 = "oink".getBytes("UTF8");
        int length1 = 1/*type*/ + 8/*timestamp*/ + 2/*key size*/ + 4/* payload size*/ + key1.length() + payload1.length;

        assertEquals(1000L, mf.append(ts0, key0, ByteBuffer.wrap(payload0)));
        assertEquals(1000L + length0, mf.append(ts1, key1, ByteBuffer.wrap(payload1)));

        int expectedLength = 4096/*file header*/ + length0 + length1;
        assertEquals(expectedLength, mf.length());
        mf.close();
        assertEquals(expectedLength, file.length());

        DataInputStream ins = new DataInputStream(new FileInputStream(file));

        assertEquals((short)0xBE01, ins.readShort());   // magic
        assertEquals((short)0, ins.readShort());        // reserved
        assertEquals(1000000, ins.readInt());           // max file size
        assertEquals(expectedLength, ins.readInt());    // checkpoint
        assertEquals(0, ins.readInt());                 // reserved

        assertEquals((int)(ts0 / 1000), ins.readInt()); // bucket time
        assertEquals(0, ins.readInt());                 // bucket first message id (relative to file)
        assertEquals(2, ins.readInt());                 // bucket count

        for (int i = 16 + 12; i < 4096; i++) {
            byte b = ins.readByte();
            assertEquals("Byte at " + i + " is not zero (0x" + Integer.toHexString(b) + ")", (byte)0, b);
        }

        assertEquals((byte)0xA1, ins.readByte());   // type
        assertEquals(ts0, ins.readLong());
        assertEquals(key0.length(), (int)ins.readShort());
        assertEquals(payload0.length, ins.readInt());
        assertEquals(key0, readUTF8(ins, key0.length()));
        assertEquals(new String(payload0, "UTF8"), readUTF8(ins, payload0.length));

        assertEquals((byte)0xA1, ins.readByte());   // type
        assertEquals(ts1, ins.readLong());
        assertEquals(key1.length(), (int)ins.readShort());
        assertEquals(payload1.length, ins.readInt());
        assertEquals(key1, readUTF8(ins, key1.length()));
        assertEquals(new String(payload1, "UTF8"), readUTF8(ins, payload1.length));

        ins.close();
    }

    private String readUTF8(InputStream ins, int length) throws IOException {
        return new String(readBytes(ins, length), "UTF8");
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private byte[] readBytes(InputStream ins, int length) throws IOException {
        byte[] buf = new byte[length];
        assertEquals(length, ins.read(buf));
        return buf;
    }

    @Test
    public void testCheckpoint() throws IOException {
        File file = new File(dir, "checkpoint.qdb");
        file.delete();
        MessageFile mf = new MessageFile(file, 0, 1000000);
        mf.append(System.currentTimeMillis(), "", ByteBuffer.wrap("oink".getBytes("UTF8")));
        mf.checkpoint();
        mf.close();

        DataInputStream ins = new DataInputStream(new FileInputStream(file));
        int expectedLength = (int) file.length();
        ins.skip(2 + 2 + 4);
        assertEquals(expectedLength, ins.readInt());
        ins.close();

        FileOutputStream out = new FileOutputStream(file, true);
        out.write("junk".getBytes("UTF8"));
        out.close();

        assertEquals(expectedLength + 4, file.length());
        new MessageFile(file, 0, 1000000).close();
        assertEquals(expectedLength, file.length());
    }

    /*
    @Test
    public void testRead() throws IOException {
        File file = new File(dir, "read.qdb");
        file.delete();
        MessageFile mf = new MessageFile(file, 1000);

        try {
            mf.cursor(999);     // before start
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException ignore) {
        }

        assertFalse(mf.cursor(1000).next());    // base offset
        assertFalse(mf.cursor(1004).next());    // first message id

        try {
            mf.cursor(1005);    // after end
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException ignore) {
        }

        long ts0 = System.currentTimeMillis();
        String key0 = "foo";
        byte[] payload0 = "piggy".getBytes("UTF8");
        long id0 = mf.append(ts0, key0, ByteBuffer.wrap(payload0));

        long ts1 = ts0 + 1;
        String key1 = "foobar";
        byte[] payload1 = "oink".getBytes("UTF8");
        long id1 = mf.append(ts1, key1, ByteBuffer.wrap(payload1));

        MessageFile.Cursor i = mf.cursor(1000);

        assertTrue(i.next());
        assertEquals(id0, i.getId());
        assertEquals(ts0, i.getTimestamp());
        assertEquals(key0, i.getRoutingKey());
        assertArrayEquals(payload0, i.getPayload());

        assertTrue(i.next());
        assertEquals(id1, i.getId());
        assertEquals(ts1, i.getTimestamp());
        assertEquals(key1, i.getRoutingKey());
        assertArrayEquals(payload1, i.getPayload());

        assertFalse(i.next());
    }

    @Test
    public void testPerformance() throws IOException {
        File file = new File(dir, "performance.qdb");
        file.delete();
        MessageFile mf = new MessageFile(file, 0);

        Random rnd = new Random(123);
        byte[] msg = new byte[4096];
        rnd.nextBytes(msg);

        int numMessages = 1000000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < numMessages; i++) {
            int sz = rnd.nextInt(msg.length);
            mf.append(System.currentTimeMillis(), "msg" + i, ByteBuffer.wrap(msg, 0, sz));
        }
        mf.checkpoint();
        mf.close();

        int ms = (int)(System.currentTimeMillis() - start);
        double perSec = numMessages / (ms / 1000.0);
        System.out.println("Write " + numMessages + " in " + ms + " ms, " + perSec + " messages per second");

        mf = new MessageFile(file, 0);
        start = System.currentTimeMillis();
        int c = 0;
        for (MessageFile.Cursor i = mf.cursor(0); i.next(); c++) {
            i.getId();
            i.getTimestamp();
            i.getRoutingKey();
            i.getPayload();
        }

        ms = (int)(System.currentTimeMillis() - start);
        perSec = c / (ms / 1000.0);
        System.out.println("Read " + c + " in " + ms + " ms, " + perSec + " messages per second");
    }
    */

}