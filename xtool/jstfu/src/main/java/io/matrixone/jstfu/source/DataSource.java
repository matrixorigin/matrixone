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

/** A named datasource that can stream its content as CSV chunks. */
public interface DataSource {
    /**
     * Stream the content as CSV chunks into {@code ctx}.  Every chunk must
     * contain only complete records (no record spans two chunks).
     *
     * @param filter pushdown hint as MySQL-dialect SQL text, "" if none; a
     *               source that cannot evaluate it must ignore it
     * @param ctx    receives chunks (with backpressure), reports cancellation,
     *               and registers resources to close on cancel
     */
    void stream(String filter, StreamContext ctx) throws Exception;

    /** Sink for complete-record CSV chunks. */
    @FunctionalInterface
    interface ChunkWriter {
        void write(byte[] data) throws Exception;
    }

    /**
     * Per-request streaming context.  {@code write} applies consumer flow
     * control (it blocks until the gRPC stream is ready, so a slow MO consumer
     * bounds jstfu's buffering instead of OOMing it) and throws once the call
     * is cancelled.  A source must also register any long-lived resource
     * (JDBC connection, file stream) so a cancelled MO query closes it even
     * while blocked in {@code getConnection}/{@code executeQuery}/{@code next}.
     */
    interface StreamContext extends ChunkWriter {
        boolean isCancelled();

        void registerForClose(AutoCloseable resource);
    }
}
