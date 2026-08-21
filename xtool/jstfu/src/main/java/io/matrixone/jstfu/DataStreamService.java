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

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/** gRPC service: looks up the datasource by name and streams its chunks. */
public class DataStreamService extends DataStreamGrpc.DataStreamImplBase {
    private static final Logger log = Logger.getLogger(DataStreamService.class.getName());

    private final Map<String, DataSource> sources;

    public DataStreamService(Map<String, DataSource> sources) {
        this.sources = sources;
    }

    @Override
    public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
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

        try {
            source.stream(request.getFilter(), data -> {
                if (observer.isCancelled()) {
                    throw new IOException("client cancelled the stream");
                }
                observer.onNext(ReadResponse.newBuilder()
                        .setChunk(Chunk.newBuilder().setData(ByteString.copyFrom(data)))
                        .build());
            });
            observer.onCompleted();
        } catch (Exception e) {
            log.log(Level.WARNING, "datasource '" + table + "' failed", e);
            if (!observer.isCancelled()) {
                observer.onNext(errorResponse(ErrorCode.ERROR_DATASOURCE_ERROR,
                        String.valueOf(e.getMessage())));
                observer.onCompleted();
            }
        }
    }

    private static ReadResponse errorResponse(ErrorCode code, String message) {
        return ReadResponse.newBuilder()
                .setError(Error.newBuilder().setCode(code).setMessage(message))
                .build();
    }
}
