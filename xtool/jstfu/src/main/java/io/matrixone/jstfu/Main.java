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

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.matrixone.jstfu.source.DataSource;
import io.matrixone.jstfu.source.FileSource;
import io.matrixone.jstfu.source.JdbcSource;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * jstfu — Java STream Fast Upload: the reference gRPC server for MatrixOne
 * datastream external tables.
 *
 * <p>Usage: {@code java -jar jstfu.jar /path/to/config.json}</p>
 */
public class Main {
    private static final Logger log = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("usage: java -jar jstfu.jar <config.json>");
            System.exit(2);
        }
        Config config = Config.load(new File(args[0]));

        Map<String, DataSource> sources = new HashMap<>();
        for (Config.DataSourceConfig ds : config.datasource) {
            switch (ds.type) {
                case "jdbc":
                    sources.put(ds.name, new JdbcSource(ds, config.chunkSize));
                    break;
                case "file":
                    sources.put(ds.name, new FileSource(ds.path, config.chunkSize));
                    break;
                default:
                    throw new IllegalStateException("unreachable: validated type " + ds.type);
            }
        }

        DataStreamService service = new DataStreamService(sources);
        Server server = NettyServerBuilder
                .forAddress(new InetSocketAddress(config.host, config.port))
                .addService(service)
                .build()
                .start();
        log.info("jstfu listening on " + config.host + ":" + config.port
                + " with " + sources.size() + " datasource(s)");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
            service.shutdown();
        }));
        server.awaitTermination();
    }
}
