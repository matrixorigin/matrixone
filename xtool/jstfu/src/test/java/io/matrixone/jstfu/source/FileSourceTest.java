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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileSourceTest {

    private static List<byte[]> chunk(String content, int chunkSize) throws IOException {
        List<byte[]> chunks = new ArrayList<>();
        FileSource.chunkStream(
                new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), chunkSize, chunks::add);
        return chunks;
    }

    private static String join(List<byte[]> chunks) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] c : chunks) {
            out.write(c);
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    @Test
    void neverSplitsARecord() throws Exception {
        StringBuilder content = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            content.append(i).append(",\"multi\nline,value\"\n");
        }
        List<byte[]> chunks = chunk(content.toString(), 64);
        assertTrue(chunks.size() > 1);
        for (byte[] c : chunks) {
            String text = new String(c, StandardCharsets.UTF_8);
            assertTrue(text.endsWith("\n"));
            // a chunk boundary must not fall inside quotes: every chunk has an
            // even number of unescaped quotes
            long quotes = text.chars().filter(ch -> ch == '"').count();
            assertEquals(0, quotes % 2, "chunk splits a quoted field:\n" + text);
        }
        assertEquals(content.toString(), join(chunks));
    }

    @Test
    void handlesEscapedQuotesAndMissingTrailingNewline() throws Exception {
        String content = "1,\"a \\\" quoted\"\n2,plain";
        List<byte[]> chunks = chunk(content, 4);
        assertEquals(content, join(chunks));
    }

    @Test
    void rejectsFileEndingInsideQuotes() {
        assertThrows(IOException.class, () -> chunk("1,\"unterminated\n", 1024));
    }

    @Test
    void emptyFileYieldsNoChunks() throws Exception {
        assertEquals(0, chunk("", 1024).size());
    }
}
