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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Encodes rows in the MatrixOne datastream CSV dialect (comma separator,
 * double-quote enclosure, backslash escape, '\n' records, NULL as unquoted
 * {@code \N}) and groups complete records into chunks of roughly the target
 * size.
 */
public class CsvChunker {
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private final int chunkSize;
    private final DataSource.ChunkSink sink;

    public CsvChunker(int chunkSize, DataSource.ChunkSink sink) {
        this.chunkSize = chunkSize;
        this.sink = sink;
    }

    /** Append one record; values may be null (encoded as \N). */
    public void addRow(String[] values) throws IOException {
        StringBuilder record = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                record.append(',');
            }
            record.append(encodeField(values[i]));
        }
        record.append('\n');
        buffer.write(record.toString().getBytes(StandardCharsets.UTF_8));
        if (buffer.size() >= chunkSize) {
            flush();
        }
    }

    /** Flush any buffered records as a final chunk. */
    public void finish() throws IOException {
        flush();
    }

    private void flush() throws IOException {
        if (buffer.size() > 0) {
            sink.chunk(buffer.toByteArray());
            buffer.reset();
        }
    }

    static String encodeField(String value) {
        if (value == null) {
            return "\\N";
        }
        // Backslash is the escape character in the MO CSV dialect: always
        // double it, then enclose when the value contains structure characters.
        String escaped = value.replace("\\", "\\\\");
        boolean needQuote = escaped.indexOf(',') >= 0
                || escaped.indexOf('"') >= 0
                || escaped.indexOf('\n') >= 0
                || escaped.indexOf('\r') >= 0;
        if (!needQuote) {
            return escaped;
        }
        return '"' + escaped.replace("\"", "\\\"") + '"';
    }
}
