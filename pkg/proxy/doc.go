// Copyright 2021 - 2023 Matrix Origin
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

/*
Package proxy is used to route client connection to specified CN server and
keep balance between CN servers.

1. Login
AS MO use MySQL protocol, the handshake process is needed during the login phase.
The general process is as follows:
(1) The client sends a TCP connection to the proxy.
(2) The proxy sends the init handshake package to the client.
(3) The client sends the handshake response to the proxy, including user name, auth
information, connection attributes, etc., and the proxy will save this part of the
information and use it when switching CN servers in the future.
(4) According to the login information of the client, the proxy selects a CN server
to establish a connection, and uses the handshake response just saved for verification.
(5) If the verification is successful, return the client OK package, otherwise return
an error package.

2. Connection management
When a client connects to the proxy, its connection will be saved and managed, and the
number of connections established on each CN server will be recorded in real time.
This information is used for CN server selection. When a new connection arrives and
the proxy tries to select a CN server, it will choose the CN server with the fewest
connections to establish the connection.

3. Data interaction
Proxy uses MySQL packet as the basic unit for forwarding, and internally maintains a buffer.
When receiving data from the client, at least one complete MySQL packet is received before
forwarding to the CN server. Conversely, the same is true for data received from the server.
Therefore, it can be observed internally whether the current connection is in the process of
executing a transaction.

4. Connection balance
Proxy will regularly load balance the connections on the CN servers. When there are too many
connections on a certain CN server, it will select these connections and try to migrate them
to other CN servers. The migration process is transparent to the client.

A security judgment will be made before attempting to migrate:
(1) Ignore if migration is in progress.
(2) The message returned by the server must be later than the client's request, otherwise
the migration cannot be done.
(3) If you are in a transaction, you cannot do migration. Tracking of transaction state is
recorded as data is interacted.

5. Scaling:
The proxy module regularly checks the work state of the cn service. If the state is draining,
then migrate the tunnels on the cn to appropriate cns. If no suitable cn is found, an error
will be reported and the original connection will become unavailable.

6. Usage
Proxy is mainly used on the cloud platform. If you want to use proxy locally, you need to
add configuration -with-proxy to start the proxy module in launch configuration mode, and
add configuration in CN config file:
[cn.frontend]
proxy-enabled = true

The default port is of proxy service 6009.

After startup, connect to port 6001 to log in normally, and then use the internal command
to configure the label of cn:

select mo_ctl('cn', 'label', 'cn_uuid:key:value');

Examples:

1. configure tenant information:

mysql -h127.0.0.1 -udump -P6001 -p111

create account acc1 admin_name 'root' identified by '111';
select mo_ctl('cn', 'label', 'dd1dccb4-4d3c-41f8-b482-5251dc7a41bf:account:acc1');

Then you can connect to the proxy to log in to the cluster:

mysql -h127.0.0.1 -uacc1:root -P6009 -p111

2. Common labels

Common labels need to pass to MO server with JDBC connection:

jdbc:mysql://localhost:6009/db123?serverTimezone=UTC&connectionAttributes=key1:value1

The labels on CN server need to be set through mo_ctl in local env or yaml file in cloud env.

Common labels also is supported by mysql client:

mysql -h127.0.0.1 -udump?k1:v1,k2:v2 -P6009 -p111
*/
package proxy
