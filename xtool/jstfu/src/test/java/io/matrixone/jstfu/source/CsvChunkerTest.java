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

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CsvChunkerTest {

    @Test
    void encodesPlainNullAndSpecialFields() {
        assertEquals("abc", CsvChunker.encodeField("abc"));
        assertEquals("\\N", CsvChunker.encodeField(null));
        assertEquals("\"a,b\"", CsvChunker.encodeField("a,b"));
        assertEquals("\"a\\\"b\"", CsvChunker.encodeField("a\"b"));
        assertEquals("\"a\nb\"", CsvChunker.encodeField("a\nb"));
        // backslash always doubled so \N round-trips as the literal text
        assertEquals("\\\\N", CsvChunker.encodeField("\\N"));
        assertEquals("", CsvChunker.encodeField(""));
    }

    @Test
    void chunksOnlyAtRecordBoundaries() throws Exception {
        List<byte[]> chunks = new ArrayList<>();
        CsvChunker chunker = new CsvChunker(10, chunks::add);
        for (int i = 0; i < 100; i++) {
            chunker.addRow(new String[]{String.valueOf(i), "value,with comma"});
        }
        chunker.finish();

        assertTrue(chunks.size() > 1);
        StringBuilder all = new StringBuilder();
        for (byte[] chunk : chunks) {
            String text = new String(chunk, StandardCharsets.UTF_8);
            // every chunk ends on a record boundary
            assertTrue(text.endsWith("\n"), "chunk must end with a newline");
            all.append(text);
        }
        String[] lines = all.toString().split("\n");
        assertEquals(100, lines.length);
        assertEquals("0,\"value,with comma\"", lines[0]);
        assertEquals("99,\"value,with comma\"", lines[99]);
    }
}
