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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Server configuration, loaded from a JSON file:
 *
 * <pre>
 * {
 *   "port": 4444,
 *   "chunksize": 1048576,
 *   "datasource": [
 *     { "name": "t1", "type": "jdbc", "connectionstring": "jdbc:mysql://...",
 *       "user": "u", "password": "p",
 *       "sql": "select a, b from src where ${FILTER}" },
 *     { "name": "t2", "type": "file", "path": "/data/t2.csv" }
 *   ]
 * }
 * </pre>
 */
public class Config {
    public static final int DEFAULT_CHUNK_SIZE = 1024 * 1024;

    @JsonProperty("port")
    public int port;

    // Bind address. Defaults to loopback so a datastream server is not exposed
    // on the network out of the box: the ${FILTER} text is substituted into
    // SQL and requests are unauthenticated, so an off-box client that reached
    // the port could run arbitrary configured-credential SQL. Co-located MO
    // (compose sidecar shares the CN netns; launch runs on the same host)
    // reaches it on 127.0.0.1. Set "0.0.0.0" (or a specific NIC) only behind a
    // network trust boundary you control.
    @JsonProperty("host")
    public String host = "127.0.0.1";

    @JsonProperty("chunksize")
    public int chunkSize = DEFAULT_CHUNK_SIZE;

    // Optional shared-secret API key. When non-empty, every Read request must
    // present a matching api_key or the server replies ERROR_UNAUTHENTICATED.
    // Empty (the default) disables the check. This is the enforcement boundary
    // for the unauthenticated-by-default surface; combine with the loopback
    // host bind when the server is exposed beyond the local MO.
    @JsonProperty("apikey")
    public String apiKey = "";

    @JsonProperty("datasource")
    public List<DataSourceConfig> datasource = new ArrayList<>();

    public static class DataSourceConfig {
        @JsonProperty("name")
        public String name;

        @JsonProperty("type")
        public String type;

        // jdbc
        @JsonProperty("connectionstring")
        public String connectionString;

        @JsonProperty("user")
        public String user;

        @JsonProperty("password")
        public String password;

        @JsonProperty("sql")
        public String sql;

        // file
        @JsonProperty("path")
        public String path;
    }

    public static Config load(File file) throws IOException {
        Config config = new ObjectMapper().readValue(file, Config.class);
        config.validate();
        return config;
    }

    void validate() {
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("config: port must be in (0, 65535], got " + port);
        }
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("config: host must not be empty");
        }
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("config: chunksize must be positive, got " + chunkSize);
        }
        Set<String> names = new HashSet<>();
        for (DataSourceConfig ds : datasource) {
            if (ds.name == null || ds.name.isEmpty()) {
                throw new IllegalArgumentException("config: datasource without a name");
            }
            if (!names.add(ds.name)) {
                throw new IllegalArgumentException("config: duplicate datasource name '" + ds.name + "'");
            }
            if (ds.type == null) {
                throw new IllegalArgumentException("config: datasource '" + ds.name + "' has no type");
            }
            switch (ds.type) {
                case "jdbc":
                    if (isEmpty(ds.connectionString) || isEmpty(ds.sql)) {
                        throw new IllegalArgumentException(
                                "config: jdbc datasource '" + ds.name + "' requires connectionstring and sql");
                    }
                    break;
                case "file":
                    if (isEmpty(ds.path)) {
                        throw new IllegalArgumentException(
                                "config: file datasource '" + ds.name + "' requires path");
                    }
                    break;
                default:
                    throw new IllegalArgumentException(
                            "config: datasource '" + ds.name + "' has unknown type '" + ds.type + "'");
            }
        }
    }

    private static boolean isEmpty(String s) {
        return s == null || s.isEmpty();
    }
}
