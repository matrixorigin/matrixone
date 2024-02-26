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
Implement the remote cache of the file service between CN nodes.
When the cache of the file service on this CN node misses, try to
send a cache read request to the CN node which has the key and
return the cached data.

If the remote node hits the cache, the data will not be read from S3.
Otherwise, continue to read data from S3.

The broadcast of the cache key is done between CN nodes through
the gossip protocol. In the case that the cluster is running normally,
the CN node sends the added/deleted cache key and broadcasts it
o all other nodes through gossip. When a CN node joins, it will select
one from the original CN node to copy the whole cache key data,
and continue to update the newly added/deleted cache key. When
one CN node goes offline, the data on all nodes will be updated
through the dead node event, and the cache key information on
the corresponding node will be removed.

The process likes the following:
1. Client sends a read request to a CN instance.
2. CN reads the data from local file service, which will probably hit
   the local cache(memory or disk).
3. But, if it does not hit local cache and if remote cache is enabled,
   it will gets remote target
   from key router, which is implemented by gossip protocol.
4. The local file service sends a remote cache read request to the target
   which is got from gossip.
5. The remote file service instance sends back cache data response.
6. If the remote cache hits, the local file service will set the data in
   the vector; otherwise, it will read from S3.

     +--------------+
     | read request |
     +--------------+
            |
            v
   +------------------+           +------------------+          +------------------+
   |        CN        |           |        CN        |          |        CN        |
   |  +------------+  |           |  +------------+  |          |  +------------+  |
   |  |   gossip   |  |           |  |   gossip   |  |          |  |   gossip   |  |
   |  |    keys    |  |           |  |    keys    |  |          |  |    keys    |  |
   |  +------------+  |           |  +------------|  |          |  +------------|  |
   |          ^  |    |           |                  |          |                  |
   |          |  |    |           |                  |          |                  |
   +----------|--|----+           +------------------+          +------------------+
          |   |  |                          |                            |
          |  key |                          |                            |
          |   |  +--target--+               |                            |
          v   +-------+     |               v                            v
   +--------------+   |     |        +--------------+             +--------------+
   | file-service |---+     |        | file-service |             | file-service |
   |              |<--------|        +--------------+             +--------------+
   +--------------+                                                     ^  |
        ^   |                                                           |  |
        |   +-----------------request remote cache target---------------+  |
        +------------------------cache data response-----------------------+

The gossip nodes in cluster works:

                                                                                                 +-------------------+
         CN1                               CN2                           CN3                     |       new CN4     |
   +-------------+                   +-------------+                +-------------+              |  +-------------+  |
   | gossip node |                   | gossip node |                | gossip node |--all items-->|  | gossip node |  |
   +-------------+                   +-------------+                +-------------+              |  +-------------+  |
       ^      ^ |                       ^   ^   |                       ^     ^                  |                   |
       |      | |                       |   |   |                       |     |                  |                   |
   set/evict  | +--[item->target(CN1)]--+   |   +--[item->target(CN1)]--+     |                  |                   |
     item     |                             |                                 |                  +------------------ +
       |      +-------+                     |                                 |                       delete CN 4
       |              |                     |                                 |                           |
   +--------------+   +---------------------+-------------------------------remove [items->target(CN4)]---+
   | file service |
   +--------------+

*/

package queryservice
