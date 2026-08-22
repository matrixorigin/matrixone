// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.matrixone.jstfu;

import io.grpc.stub.ServerCallStreamObserver;
import io.matrixone.datastream.v1.ErrorCode;
import io.matrixone.datastream.v1.ReadRequest;
import io.matrixone.datastream.v1.ReadResponse;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataStreamServiceTest {

    // Captures responses. The auth-reject and unknown-table paths run
    // synchronously on the calling thread before any streaming machinery, so
    // only onNext/onCompleted are exercised here.
    static final class Capture extends ServerCallStreamObserver<ReadResponse> {
        final List<ReadResponse> responses = new ArrayList<>();
        boolean completed;

        @Override
        public void onNext(ReadResponse value) {
            responses.add(value);
        }

        @Override
        public void onCompleted() {
            completed = true;
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setOnCancelHandler(Runnable r) {
        }

        @Override
        public void setCompression(String c) {
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setOnReadyHandler(Runnable r) {
        }

        @Override
        public void request(int n) {
        }

        @Override
        public void setMessageCompression(boolean e) {
        }

        @Override
        public void disableAutoInboundFlowControl() {
        }
    }

    private static DataStreamService service(String apiKey) {
        // no datasources: an authenticated request resolves to TABLE_NOT_FOUND,
        // which is enough to prove the auth gate let it through
        return new DataStreamService(Collections.emptyMap(), apiKey);
    }

    private static ErrorCode errorOf(Capture c) {
        assertTrue(c.completed);
        assertEquals(1, c.responses.size());
        return c.responses.get(0).getError().getCode();
    }

    @Test
    void authDisabledPassesThrough() {
        Capture c = new Capture();
        service("").read(ReadRequest.newBuilder().setTable("nope").build(), c);
        assertEquals(ErrorCode.ERROR_TABLE_NOT_FOUND, errorOf(c));
    }

    @Test
    void wrongKeyIsRejected() {
        Capture c = new Capture();
        service("s3cr3t").read(
                ReadRequest.newBuilder().setTable("nope").setApiKey("wrong").build(), c);
        assertEquals(ErrorCode.ERROR_UNAUTHENTICATED, errorOf(c));
    }

    @Test
    void missingKeyIsRejected() {
        Capture c = new Capture();
        service("s3cr3t").read(ReadRequest.newBuilder().setTable("nope").build(), c);
        assertEquals(ErrorCode.ERROR_UNAUTHENTICATED, errorOf(c));
    }

    @Test
    void correctKeyPassesAuth() {
        Capture c = new Capture();
        service("s3cr3t").read(
                ReadRequest.newBuilder().setTable("nope").setApiKey("s3cr3t").build(), c);
        // passed auth, then failed to find the (absent) datasource
        assertEquals(ErrorCode.ERROR_TABLE_NOT_FOUND, errorOf(c));
    }
}
