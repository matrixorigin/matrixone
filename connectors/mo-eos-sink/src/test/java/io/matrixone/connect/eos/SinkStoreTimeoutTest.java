package io.matrixone.connect.eos;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

final class SinkStoreTimeoutTest {
    @Test
    void stalledMysqlGreetingReleasesJdbcCaller() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                CountDownLatch accepted = new CountDownLatch(1);
                Future<?> peer = executor.submit(() -> {
                    try (Socket socket = server.accept()) {
                        accepted.countDown();
                        // Accept TCP but never send a MySQL greeting.
                        while (!Thread.currentThread().isInterrupted() && !socket.isClosed()) {
                            Thread.sleep(100);
                        }
                    } catch (Exception ignored) {
                        // Test cleanup closes the socket/server.
                    }
                });
                Map<String, String> props = SinkSettingsTest.validProps(
                    "jdbc:mysql://127.0.0.1:" + server.getLocalPort() + "/profile?useSSL=false");
                props.put(AtomicSinkConnector.JDBC_CONNECT_TIMEOUT, "250");
                props.put(AtomicSinkConnector.JDBC_SOCKET_TIMEOUT, "250");
                SinkStore store = new SinkStore(SinkSettings.from(props));
                Future<?> caller = executor.submit(() -> store.read(new TopicPartition("events", 0)));
                try {
                    ExecutionException failure = assertThrows(ExecutionException.class,
                        () -> caller.get(5, TimeUnit.SECONDS));
                    assertInstanceOf(ConnectException.class, failure.getCause());
                    assertTrue(accepted.await(1, TimeUnit.SECONDS));
                } finally {
                    caller.cancel(true);
                    peer.cancel(true);
                }
            } finally {
                executor.shutdownNow();
            }
        }
    }
}
