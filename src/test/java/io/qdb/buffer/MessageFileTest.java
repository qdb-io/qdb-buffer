package io.qdb.buffer;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.zip.CRC32;

import static junit.framework.Assert.assertEquals;

public class MessageFileTest {

    private static File dir = new File("build/test-data");

    @SuppressWarnings({"ResultOfMethodCallIgnored", "ConstantConditions"})
    @BeforeClass
    public static void setup() {
        dir.mkdir();
        assert dir.isDirectory();
        for (File c : dir.listFiles()) nuke(c);
    }

    @SuppressWarnings("ConstantConditions")
    private static void nuke(File f) {
        if (f.isDirectory()) {
            for (File c : f.listFiles()) nuke(c);
        }
        assert f.delete();
    }

    @Test
    public void testAppend() throws IOException {
        File file = new File(dir, "append.qdb");
        MessageFile mf = new MessageFile(file, 1000);

        long ts0 = System.currentTimeMillis();
        String key0 = "foo";
        byte[] payload0 = "piggy".getBytes("UTF8");
        int length0 = 4/*length*/ + 1/*type*/ + 8/*timestamp*/ + 1/*key len*/ + key0.length() + payload0.length;

        long ts1 = ts0 + 1;
        String key1 = "foobar";
        byte[] payload1 = "oink".getBytes("UTF8");
        int length1 = 4/*length*/ + 1/*type*/ + 8/*timestamp*/ + 1/*key len*/ + key1.length() + payload1.length;

        assertEquals(1004L, mf.append(ts0, key0, ByteBuffer.wrap(payload0)));
        assertEquals(1004L + length0, mf.append(ts1, key1, ByteBuffer.wrap(payload1)));

        int expectedLength = 4/*checkpoint*/ + length0 + length1;
        assertEquals(expectedLength, mf.length());
        mf.close();
        assertEquals(expectedLength, file.length());

        DataInputStream ins = new DataInputStream(new FileInputStream(file));

        assertEquals(0, ins.readInt()); // checkpoint

        assertEquals(length0, ins.readInt());
        assertEquals((byte)0xA1, ins.readByte());   // type
        assertEquals(ts0, ins.readLong());
        assertEquals(key0.length(), (int)ins.readByte() & 0xFF);
        assertEquals(key0, readUTF8(ins, key0.length()));
        assertEquals(new String(payload0, "UTF8"), readUTF8(ins, payload0.length));

        assertEquals(length1, ins.readInt());
        assertEquals((byte)0xA1, ins.readByte());   // type
        assertEquals(ts1, ins.readLong());
        assertEquals(key1.length(), (int)ins.readByte() & 0xFF);
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
        MessageFile mf = new MessageFile(file, 0);
        mf.append(System.currentTimeMillis(), "", ByteBuffer.wrap("oink".getBytes("UTF8")));
        mf.checkpoint();
        mf.close();

        DataInputStream ins = new DataInputStream(new FileInputStream(file));
        int expectedLength = (int) file.length();
        assertEquals(expectedLength, ins.readInt());
        ins.close();

        FileOutputStream out = new FileOutputStream(file, true);
        out.write("junk".getBytes("UTF8"));
        out.close();

        assertEquals(expectedLength + 4, file.length());
        new MessageFile(file, 0).close();
        assertEquals(expectedLength, file.length());
    }

    /*
    @Test
    public void testPerformance() throws IOException {
        MessageFile mf = new MessageFile(new File(dir, "performance.qdb"), 0);

        Random rnd = new Random(123);
        byte[] msg = new byte[4096];
        rnd.nextBytes(msg);

        int numMessages = 100000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < numMessages; i++) {
            int sz = rnd.nextInt(msg.length);
            mf.append(msg, 0, sz);
        }
        int ms = (int)(System.currentTimeMillis() - start);
        double perSec = numMessages / (ms / 1000.0);
        System.out.println(ms + " ms, " + perSec + " messages per second");
    }
    */

}