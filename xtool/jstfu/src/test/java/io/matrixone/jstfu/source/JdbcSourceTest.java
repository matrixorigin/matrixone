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

import java.sql.SQLDataException;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcSourceTest {

    @Test
    void substitutesFilterPlaceholder() {
        String sql = "select a from t where ${FILTER}";
        assertEquals("select a from t where (`a` > 1)",
                JdbcSource.substituteFilter(sql, "(`a` > 1)"));
        assertEquals("select a from t where 1=1", JdbcSource.substituteFilter(sql, ""));
        assertEquals("select a from t where 1=1", JdbcSource.substituteFilter(sql, null));
        assertEquals("select a from t", JdbcSource.substituteFilter("select a from t", "(`a` > 1)"));
    }

    @Test
    void formatsValues() throws Exception {
        assertNull(JdbcSource.formatValue(null));
        assertEquals("42", JdbcSource.formatValue(42));
        assertEquals("x", JdbcSource.formatValue("x"));
        assertEquals("2021-01-02 03:04:05",
                JdbcSource.formatValue(Timestamp.valueOf("2021-01-02 03:04:05")));
        assertEquals("2021-01-02 03:04:05.123000",
                JdbcSource.formatValue(Timestamp.valueOf("2021-01-02 03:04:05.123")));
    }

    @Test
    void rejectsBinaryColumnsRatherThanCorrupting() {
        // a byte >= 0x80 could not round-trip byte-for-byte through the UTF-8
        // CSV stream; fail loudly instead of silently corrupting
        assertThrows(SQLDataException.class,
                () -> JdbcSource.formatValue(new byte[]{(byte) 0xff, 0x00, (byte) 0x80}));
    }
}
