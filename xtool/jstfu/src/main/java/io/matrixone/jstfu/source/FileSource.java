// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.matrixone.jstfu.source;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Streams an existing CSV file, re-chunking it so no record spans two chunks.
 * Chunk boundaries are placed at record separators ('\n') that sit outside a
 * quoted field; quote state tracks double-quote enclosure with backslash
 * escapes (the MO CSV dialect).  The filter hint is a documented noop.
 */
public class FileSource implements DataSource {
    private final String path;
    private final int chunkSize;

    public FileSource(String path, int chunkSize) {
        this.path = path;
        this.chunkSize = chunkSize;
    }

    @Override
    public void stream(String filter, StreamContext ctx) throws Exception {
        try (InputStream in = new BufferedInputStream(new FileInputStream(path))) {
            // a cancelled MO query closes the stream, unblocking a large read
            ctx.registerForClose(in);
            chunkStream(in, chunkSize, ctx);
        }
    }

    static void chunkStream(InputStream in, int chunkSize, ChunkWriter sink) throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        boolean inQuotes = false;
        boolean escaped = false;
        int b;
        while ((b = in.read()) >= 0) {
            buffer.write(b);
            if (escaped) {
                escaped = false;
                continue;
            }
            switch (b) {
                case '\\':
                    escaped = true;
                    break;
                case '"':
                    inQuotes = !inQuotes;
                    break;
                case '\n':
                    if (!inQuotes && buffer.size() >= chunkSize) {
                        sink.write(buffer.toByteArray());
                        buffer.reset();
                    }
                    break;
                default:
            }
        }
        if (inQuotes) {
            throw new IOException("file ends inside a quoted CSV field");
        }
        if (buffer.size() > 0) {
            sink.write(buffer.toByteArray());
        }
    }
}
