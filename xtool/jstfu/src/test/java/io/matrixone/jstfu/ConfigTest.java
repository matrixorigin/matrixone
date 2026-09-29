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

package io.matrixone.jstfu;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConfigTest {

    private static Config load(String json) throws IOException {
        File file = File.createTempFile("jstfu", ".json");
        file.deleteOnExit();
        Files.write(file.toPath(), json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return Config.load(file);
    }

    @Test
    void loadsValidConfig() throws Exception {
        Config config = load("{\"port\": 4444, \"datasource\": ["
                + "{\"name\": \"j\", \"type\": \"jdbc\", \"connectionstring\": \"jdbc:mysql://h\","
                + " \"user\": \"u\", \"password\": \"p\", \"sql\": \"select 1 where ${FILTER}\"},"
                + "{\"name\": \"f\", \"type\": \"file\", \"path\": \"/tmp/x.csv\"}"
                + "]}");
        assertEquals(4444, config.port);
        assertEquals(Config.DEFAULT_CHUNK_SIZE, config.chunkSize);
        assertEquals(2, config.datasource.size());
    }

    @Test
    void rejectsInvalidConfigs() {
        assertThrows(Exception.class, () -> load("{\"port\": 0}"));
        assertThrows(Exception.class, () -> load(
                "{\"port\": 1, \"datasource\": [{\"name\": \"a\", \"type\": \"nope\"}]}"));
        assertThrows(Exception.class, () -> load(
                "{\"port\": 1, \"datasource\": [{\"type\": \"file\", \"path\": \"/x\"}]}"));
        assertThrows(Exception.class, () -> load(
                "{\"port\": 1, \"datasource\": [{\"name\": \"a\", \"type\": \"file\"}]}"));
        assertThrows(Exception.class, () -> load(
                "{\"port\": 1, \"datasource\": [{\"name\": \"a\", \"type\": \"jdbc\", \"connectionstring\": \"c\"}]}"));
        // duplicate names
        assertThrows(Exception.class, () -> load("{\"port\": 1, \"datasource\": ["
                + "{\"name\": \"a\", \"type\": \"file\", \"path\": \"/x\"},"
                + "{\"name\": \"a\", \"type\": \"file\", \"path\": \"/y\"}"
                + "]}"));
    }
}
