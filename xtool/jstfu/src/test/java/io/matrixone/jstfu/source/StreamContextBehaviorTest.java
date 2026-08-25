// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.matrixone.jstfu.source;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamContextBehaviorTest {

    /** A StreamContext that records chunks, can pre-arm cancellation, and
     *  tracks registered closeables. */
    static final class FakeCtx implements DataSource.StreamContext {
        final List<byte[]> chunks = new ArrayList<>();
        final List<AutoCloseable> registered = new ArrayList<>();
        volatile boolean cancelled;
        int cancelAfter = Integer.MAX_VALUE;

        @Override
        public void write(byte[] data) throws Exception {
            // mirror production StreamCtx: a cancelled write throws, so the
            // source's chunk loop aborts
            if (cancelled) {
                throw new IllegalStateException("cancelled");
            }
            chunks.add(data);
            if (chunks.size() >= cancelAfter) {
                cancelled = true;
            }
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public void registerForClose(AutoCloseable resource) {
            registered.add(resource);
        }
    }

    @Test
    void fileSourceRegistersItsStreamForClose() throws Exception {
        File f = File.createTempFile("jstfu", ".csv");
        f.deleteOnExit();
        Files.write(f.toPath(), "1,a\n2,b\n".getBytes(StandardCharsets.UTF_8));

        FakeCtx ctx = new FakeCtx();
        new FileSource(f.getAbsolutePath(), 1024).stream("", ctx);

        // the input stream is registered so a cancel can close it mid-read
        assertEquals(1, ctx.registered.size());
        // whole file delivered
        int total = ctx.chunks.stream().mapToInt(c -> c.length).sum();
        assertEquals(8, total);
    }

    @Test
    void cancelledChunkWriteStopsFileStreaming() throws Exception {
        // a file large enough to produce several small chunks
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            sb.append(i).append(",row\n");
        }
        File f = File.createTempFile("jstfu", ".csv");
        f.deleteOnExit();
        Files.write(f.toPath(), sb.toString().getBytes(StandardCharsets.UTF_8));

        FakeCtx ctx = new FakeCtx();
        ctx.cancelAfter = 1; // cancel after the first chunk

        // the sink refuses further writes once cancelled, aborting the loop;
        // FileSource must not deliver the whole file
        try {
            new FileSource(f.getAbsolutePath(), 8).stream("", ctx);
        } catch (Exception ignored) {
            // a cancel surfaces as an exception from the sink in production;
            // the fake sink simply flips the flag, so completion is also fine
        }
        assertTrue(ctx.cancelled);
        // far fewer chunks than the ~125 a full 8-byte-chunked run would yield
        assertFalse(ctx.chunks.size() > 50, "streaming did not stop after cancel");
    }
}
