/*
 * Copyright 2012 David Tinker
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.qdb.buffer;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.Set;

/**
 * Closes all open buffers on shutdown.
 */
class ShutdownHook extends Thread {

    private static ShutdownHook instance;
    static {
        Runtime.getRuntime().addShutdownHook(instance = new ShutdownHook());
    }

    public static ShutdownHook get() {
        return instance;
    }

    ShutdownHook() {
        super("qdb-shutdown-hook");
    }

    private final Set<Reference<MessageBuffer>> set = new HashSet<Reference<MessageBuffer>>();

    public synchronized Reference<MessageBuffer> register(MessageBuffer mb) {
        WeakReference<MessageBuffer> ref = new WeakReference<MessageBuffer>(mb);
        set.add(ref);
        return ref;
    }

    public synchronized void unregister(Reference<MessageBuffer> ref) {
        set.remove(ref);
    }

    @Override
    public void run() {
        Reference[] todo;
        synchronized (this) {
            int n = set.size();
            if (n == 0) return;
            set.toArray(todo = new Reference[n]);
        }
        for (Reference ref : todo) {
            MessageBuffer mb = (MessageBuffer)ref.get();
            if (mb != null) {
                try {
                    mb.close();
                } catch (IOException e) {
                    // todo should probably log this somewhere
                }
            }
        }
    }
}
