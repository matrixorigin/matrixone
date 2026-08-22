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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * gRPC service: looks up the datasource by name and streams its chunks.
 *
 * <p>The source is drained on a worker thread so the call's readiness and
 * cancellation callbacks (which run on the gRPC executor) can drive it: the
 * worker blocks in {@link StreamCtx#write} until the client stream is ready
 * (bounded buffering) and stops promptly on cancel.</p>
 *
 * <p>Cancellation, worker publication, and resource registration are made
 * atomic by {@link Cancellation}: registering a resource after the call is
 * cancelled closes it immediately, cancelling closes everything registered so
 * far, and the worker future is re-checked after publication so a cancel that
 * raced submission still interrupts it. The worker pool is bounded so stalled
 * or cancelled requests cannot grow threads without limit.</p>
 */
public class DataStreamService extends DataStreamGrpc.DataStreamImplBase {
    private static final Logger log = Logger.getLogger(DataStreamService.class.getName());

    private final Map<String, DataSource> sources;
    private final byte[] apiKey;
    private final ExecutorService workers;

    public DataStreamService(Map<String, DataSource> sources, String apiKey, int maxConcurrentReads) {
        this.sources = sources;
        this.apiKey = (apiKey == null || apiKey.isEmpty())
                ? null
                : apiKey.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int max = Math.max(1, maxConcurrentReads);
        // 0 core, bounded max, SynchronousQueue: a cached pool that never
        // exceeds `max` live worker threads; excess reads are rejected rather
        // than queued or spawning unbounded threads.
        this.workers = new ThreadPoolExecutor(0, max, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                r -> {
                    Thread t = new Thread(r, "jstfu-read");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.AbortPolicy());
    }

    /** Stops the worker pool; call on server shutdown. */
    public void shutdown() {
        workers.shutdownNow();
    }

    static final class StreamCancelledException extends RuntimeException {
    }

    @Override
    public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
        @SuppressWarnings("unchecked")
        ServerCallStreamObserver<ReadResponse> observer =
                (ServerCallStreamObserver<ReadResponse>) responseObserver;
        String table = request.getTable();
        log.info(() -> "read table=" + table + " filter=" + request.getFilter());

        if (!authenticated(request)) {
            observer.onNext(errorResponse(ErrorCode.ERROR_UNAUTHENTICATED,
                    "missing or invalid api key"));
            observer.onCompleted();
            return;
        }

        DataSource source = sources.get(table);
        if (source == null) {
            observer.onNext(errorResponse(ErrorCode.ERROR_TABLE_NOT_FOUND,
                    "no datasource named '" + table + "'"));
            observer.onCompleted();
            return;
        }

        final Object readyLock = new Object();
        final Cancellation cancellation = new Cancellation();
        final AtomicReference<Future<?>> workerFuture = new AtomicReference<>();

        observer.setOnReadyHandler(() -> {
            synchronized (readyLock) {
                readyLock.notifyAll();
            }
        });
        observer.setOnCancelHandler(() -> {
            cancellation.cancel();
            synchronized (readyLock) {
                readyLock.notifyAll();
            }
            Future<?> f = workerFuture.get();
            if (f != null) {
                f.cancel(true);
            }
        });

        StreamCtx ctx = new StreamCtx(observer, readyLock, cancellation);

        Runnable job = () -> {
            try {
                if (cancellation.isCancelled()) {
                    return;
                }
                source.stream(request.getFilter(), ctx);
                if (!cancellation.isCancelled()) {
                    observer.onCompleted();
                }
            } catch (StreamCancelledException | InterruptedException e) {
                // cancelled: the client is gone, nothing to send
            } catch (Exception e) {
                if (cancellation.isCancelled()) {
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
                // close anything the source registered but did not release
                cancellation.cancel();
            }
        };

        Future<?> future;
        try {
            future = workers.submit(job);
        } catch (RejectedExecutionException rej) {
            observer.onNext(errorResponse(ErrorCode.ERROR_DATASOURCE_ERROR,
                    "server busy: too many concurrent reads"));
            observer.onCompleted();
            return;
        }
        workerFuture.set(future);
        // Close the cancel/submit race: a cancel that fired after the handler
        // was installed but before the future was published saw a null future;
        // re-check here so it still interrupts the worker.
        if (cancellation.isCancelled()) {
            future.cancel(true);
        }
    }

    private boolean authenticated(ReadRequest request) {
        if (apiKey == null) {
            return true; // auth disabled
        }
        // constant-time comparison so a wrong key cannot be recovered by timing
        byte[] presented = request.getApiKey().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return java.security.MessageDigest.isEqual(apiKey, presented);
    }

    private static ReadResponse errorResponse(ErrorCode code, String message) {
        return ReadResponse.newBuilder()
                .setError(Error.newBuilder().setCode(code).setMessage(message))
                .build();
    }

    private static void closeQuietly(AutoCloseable c) {
        try {
            c.close();
        } catch (Exception ignore) {
            // best-effort teardown
        }
    }

    /**
     * Makes cancellation and resource registration atomic for one call.
     * Registering after cancellation closes the resource immediately (and
     * tells the caller to stop); cancelling closes everything registered so
     * far. Because both operations hold the same monitor, a resource can
     * never be registered into an already-drained set and leak.
     */
    static final class Cancellation {
        private boolean cancelled;
        private final List<AutoCloseable> closeables = new ArrayList<>();

        synchronized boolean isCancelled() {
            return cancelled;
        }

        /** @return false and closes {@code c} if already cancelled. */
        synchronized boolean register(AutoCloseable c) {
            if (cancelled) {
                closeQuietly(c);
                return false;
            }
            closeables.add(c);
            return true;
        }

        synchronized void cancel() {
            if (cancelled) {
                return;
            }
            cancelled = true;
            for (AutoCloseable c : closeables) {
                closeQuietly(c);
            }
            closeables.clear();
        }
    }

    /** Backpressure- and cancellation-aware chunk sink for one call. */
    static final class StreamCtx implements DataSource.StreamContext {
        private final ServerCallStreamObserver<ReadResponse> observer;
        private final Object readyLock;
        private final Cancellation cancellation;

        StreamCtx(ServerCallStreamObserver<ReadResponse> observer, Object readyLock,
                Cancellation cancellation) {
            this.observer = observer;
            this.readyLock = readyLock;
            this.cancellation = cancellation;
        }

        @Override
        public void write(byte[] data) throws InterruptedException {
            synchronized (readyLock) {
                while (!observer.isReady() && !cancellation.isCancelled()) {
                    readyLock.wait();
                }
            }
            if (cancellation.isCancelled()) {
                throw new StreamCancelledException();
            }
            observer.onNext(ReadResponse.newBuilder()
                    .setChunk(Chunk.newBuilder().setData(ByteString.copyFrom(data)))
                    .build());
        }

        @Override
        public boolean isCancelled() {
            return cancellation.isCancelled();
        }

        @Override
        public void registerForClose(AutoCloseable resource) {
            // close-on-late-register: if the call was already cancelled, the
            // resource is closed now and the source is told to stop instead of
            // running with a resource nothing will release.
            if (!cancellation.register(resource)) {
                throw new StreamCancelledException();
            }
        }
    }
}
