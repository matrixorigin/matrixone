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

import java.io.IOException;

/** A named datasource that can stream its content as CSV chunks. */
public interface DataSource {
    /**
     * Stream the content as CSV chunks into {@code sink}.  Every chunk must
     * contain only complete records (no record spans two chunks).
     *
     * @param filter pushdown hint as MySQL-dialect SQL text, "" if none; a
     *               source that cannot evaluate it must ignore it
     */
    void stream(String filter, ChunkSink sink) throws Exception;

    /** Receives complete-record CSV chunks. */
    interface ChunkSink {
        void chunk(byte[] data) throws IOException;
    }
}
