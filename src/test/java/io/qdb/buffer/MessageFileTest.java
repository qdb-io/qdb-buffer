package io.qdb.buffer;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

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
        MessageFile mf = new MessageFile(new File(dir, "append.qdb"));
        byte[] bytes = "wibble".getBytes("UTF8");
        mf.append(bytes, 0, bytes.length);
        mf.close();
    }

    @Test
    public void testPerformance() throws IOException {
        MessageFile mf = new MessageFile(new File(dir, "performance.qdb"));

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

}