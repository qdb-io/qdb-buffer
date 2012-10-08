package io.qdb.buffer;

import java.io.File;
import java.io.IOException;

/**
 * Implementation using memory mapped files.
 */
public class PersistentMessageBuffer implements MessageBuffer {

    private final File dir;

    public PersistentMessageBuffer(File dir) throws IOException {
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                throw new IOException("Directory [" + dir + "] does not exist and could not be created");
            }
        }
        if (!dir.isDirectory()) {
            throw new IOException("Not a directory [" + dir + "]");
        }
        this.dir = dir;
    }

    @Override
    public long add(byte[] data, int offset, int length) throws IOException {
        return 0;
    }

    @Override
    public Msg get(long id) {
        return null;
    }

    @Override
    public void remove(long id) {
    }
}
