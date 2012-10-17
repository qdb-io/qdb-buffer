package io.qdb.buffer;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Random;

import static junit.framework.Assert.*;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class ChannelInputTest {

    private static File dir = new File("build/test-data");

    @BeforeClass
    public static void beforeClass() throws IOException {
        if (!dir.isDirectory() && !dir.mkdirs()) {
            throw new IOException("Unable to create [" + dir + "]");
        }
    }

    @Test
    public void testRead() throws IOException {
        File file = new File(dir, "read.dat");
        file.delete();

        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        out.writeByte(0x23);
        out.writeShort(0x1234);
        out.writeInt(0x12345678);
        out.writeLong(0x1234567898765432L);
        // total 1 + 2 + 4 + 8 = 15 bytes

        // put a short across a buffer boundary
        for (int i = 0; i < 8192 - 15 - 1; i++) out.write(0);
        out.writeShort(0x4321);

        // put an int across the next buffer boundary
        for (int i = 0; i < 8192 - 1 - 3; i++) out.write(0);
        out.writeInt(0x1a2b3c4d);

        // put a long across the next buffer boundary
        for (int i = 0; i < 8192 - 1 - 7; i++) out.write(0);
        out.writeLong(0x1122334455667788L);

        // now write several buffers worth of data
        for (int i = 0; i < 8192 * 3; i++) out.writeByte(i);

        out.close();

        FileInputStream ins = new FileInputStream(file);
        ChannelInput in = new ChannelInput(ins.getChannel(), 0, 8192);

        assertEquals((byte)0x23, in.readByte());
        assertEquals((short)0x1234, in.readShort());
        assertEquals(0x12345678, in.readInt());
        assertEquals(0x1234567898765432L, in.readLong());

        in.skip(8192 - 15 - 1);
        assertEquals((short)0x4321, in.readShort());

        in.skip(8192 - 1 - 3);
        assertEquals(0x1a2b3c4d, in.readInt());

        in.skip(8192 - 1 - 7);
        assertEquals(0x1122334455667788L, in.readLong());

        byte[] data = new byte[8192 * 3];
        in.read(data, 0, data.length);
        for (int i = 0; i < data.length; i++) {
            assertEquals((byte)i, data[i]);
        }

        ins.close();
   }

}