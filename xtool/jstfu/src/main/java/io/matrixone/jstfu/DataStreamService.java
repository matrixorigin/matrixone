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

import com.google.protobuf.ByteString;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.matrixone.datastream.v1.Chunk;
import io.matrixone.datastream.v1.DataStreamGrpc;
import io.matrixone.datastream.v1.Error;
import io.matrixone.datastream.v1.ErrorCode;
import io.matrixone.datastream.v1.ReadRequest;
import io.matrixone.datastream.v1.ReadResponse;
import io.matrixone.jstfu.source.DataSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * gRPC service: looks up the datasource by name and streams its chunks.
 *
 * <p>The source is drained on a worker thread so the call's readiness and
 * cancellation callbacks (which run on the gRPC executor) can drive it: the
 * worker blocks in {@link StreamCtx#write} until the client stream is ready
 * (bounded buffering) and stops promptly on cancel, which also closes the
 * source's registered resources and interrupts the worker so a JDBC read
 * blocked in the driver is released rather than leaked.</p>
 */
public class DataStreamService extends DataStreamGrpc.DataStreamImplBase {
    private static final Logger log = Logger.getLogger(DataStreamService.class.getName());

    private final Map<String, DataSource> sources;
    private final ExecutorService workers;

    public DataStreamService(Map<String, DataSource> sources) {
        this.sources = sources;
        this.workers = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "jstfu-read");
            t.setDaemon(true);
            return t;
        });
    }

    /** Stops the worker pool; call on server shutdown. */
    public void shutdown() {
        workers.shutdownNow();
    }

    private static final class StreamCancelledException extends RuntimeException {
    }

    @Override
    public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
        @SuppressWarnings("unchecked")
        ServerCallStreamObserver<ReadResponse> observer =
                (ServerCallStreamObserver<ReadResponse>) responseObserver;
        String table = request.getTable();
        log.info(() -> "read table=" + table + " filter=" + request.getFilter());

        DataSource source = sources.get(table);
        if (source == null) {
            observer.onNext(errorResponse(ErrorCode.ERROR_TABLE_NOT_FOUND,
                    "no datasource named '" + table + "'"));
            observer.onCompleted();
            return;
        }

        final Object readyLock = new Object();
        final AtomicBoolean cancelled = new AtomicBoolean(false);
        final List<AutoCloseable> closeables = Collections.synchronizedList(new ArrayList<>());
        final AtomicReference<Future<?>> workerFuture = new AtomicReference<>();

        observer.setOnReadyHandler(() -> {
            synchronized (readyLock) {
                readyLock.notifyAll();
            }
        });
        observer.setOnCancelHandler(() -> {
            cancelled.set(true);
            synchronized (readyLock) {
                readyLock.notifyAll();
            }
            closeAll(closeables);
            Future<?> f = workerFuture.get();
            if (f != null) {
                f.cancel(true);
            }
        });

        StreamCtx ctx = new StreamCtx(observer, readyLock, cancelled, closeables);

        workerFuture.set(workers.submit(() -> {
            try {
                source.stream(request.getFilter(), ctx);
                if (!cancelled.get()) {
                    observer.onCompleted();
                }
            } catch (StreamCancelledException | InterruptedException e) {
                // cancelled: the client is gone, nothing to send
            } catch (Exception e) {
                if (cancelled.get()) {
                    return;
                }
                log.log(Level.WARNING, "datasource '" + table + "' failed", e);
                try {
                    observer.onNext(errorResponse(ErrorCode.ERROR_DATASOURCE_ERROR,
                            String.valueOf(e.getMessage())));
                    observer.onCompleted();
                } catch (RuntimeException ignore) {
                    // stream already torn down
                }
            } finally {
                closeAll(closeables);
            }
        }));
    }

    private static void closeAll(List<AutoCloseable> closeables) {
        synchronized (closeables) {
            for (AutoCloseable c : closeables) {
                try {
                    c.close();
                } catch (Exception ignore) {
                    // best-effort teardown
                }
            }
            closeables.clear();
        }
    }

    private static ReadResponse errorResponse(ErrorCode code, String message) {
        return ReadResponse.newBuilder()
                .setError(Error.newBuilder().setCode(code).setMessage(message))
                .build();
    }

    /** Backpressure- and cancellation-aware chunk sink for one call. */
    private static final class StreamCtx implements DataSource.StreamContext {
        private final ServerCallStreamObserver<ReadResponse> observer;
        private final Object readyLock;
        private final AtomicBoolean cancelled;
        private final List<AutoCloseable> closeables;

        StreamCtx(ServerCallStreamObserver<ReadResponse> observer, Object readyLock,
                AtomicBoolean cancelled, List<AutoCloseable> closeables) {
            this.observer = observer;
            this.readyLock = readyLock;
            this.cancelled = cancelled;
            this.closeables = closeables;
        }

        @Override
        public void write(byte[] data) throws InterruptedException {
            synchronized (readyLock) {
                while (!observer.isReady() && !cancelled.get()) {
                    readyLock.wait();
                }
            }
            if (cancelled.get()) {
                throw new StreamCancelledException();
            }
            observer.onNext(ReadResponse.newBuilder()
                    .setChunk(Chunk.newBuilder().setData(ByteString.copyFrom(data)))
                    .build());
        }

        @Override
        public boolean isCancelled() {
            return cancelled.get();
        }

        @Override
        public void registerForClose(AutoCloseable resource) {
            closeables.add(resource);
        }
    }
}
