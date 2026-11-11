# Index objects pinned in the index-file cache

## Problem

An IVF-Flat search reads its entries table, about the size of its vectors, on every query.
In the shared disk cache those objects compete with every other table: a scan of another
workload evicts them, and the next search downloads them again, while they take room the
other workload's objects then miss.

## Decision

With `[fileservice.cache] index-file-pin-objects = true` (default false), a CN keeps the
objects of IVF-Flat entries tables whole in its index-file cache, apart from the shared
disk cache.

- **What is pinned.** Reads of an entries table carry `IndexFileObject`: its table scans,
  identified by the table type, and the IVF-Flat search's own scan. A read with it of an
  object the index-file cache holds is served from there. Otherwise the object is queued
  for download into the index-file cache, and the read takes the normal path with its
  ranges alone; a whole-object prefetch with it, through the prefetcher or the IO
  pipeline, is queued the same way, not downloaded into the disk cache. An object the
  cache has no room for, or that is waiting to be queued again, keeps the disk cache's
  prefetch and whole-object read.
- **What a CN pins.** The objects its own reads touch. A distributed search assigns whole
  objects to CNs by object id, so each CN pins its share; a CN that serves the whole index
  pins all of it.
- **Consistency.** A pinned copy is the bytes of an immutable object named by the object.
  Which objects and rows a query sees still comes from its snapshot, so results do not
  depend on what is pinned; only where the bytes come from does.
- **Room.** An object is pinned only into the index-file cache's free room, counting the
  downloads under way, so pinning itself evicts nothing and cannot thrash. The cache stays
  one LRU shared with the other index files: a later index file that needs room evicts the
  least recently used entries, pinned copies included. An object not pinned, for lack of
  room or a failed download, is not queued again for a minute.
- **Downloads.** As many objects download at once per CN as the prefetcher's workers,
  each once at a time, and an object of 16 MiB or more reads four ranges at once. They
  hold connections of the disk cache's download budget. A read never waits for them.
- **Use and restart.** A read of a pinned copy marks it used. After a restart the copies
  on disk serve reads while the cache loads them and are not downloaded again.
- **Delete.** Deleting an object through the CN's file service drops its queued pin,
  waits for one under way, then deletes its pinned copy, so no read after the delete is
  served from it. Objects deleted elsewhere, as by TN garbage collection, are no longer
  read, and their copies age out.
- **Cleanup.** Pinned copies no longer read age out with the index-file cache's idle
  collection.

## Alternatives

- **Make the index-file cache the disk tier of index tables.** The read and prefetch path
  uses the disk cache directly in about forty places, and the index-file cache's LRU
  would then evict index files for entries.
- **Fill each index whole when it is first used.** A distributed search reads only a
  share of the objects on each CN, which would each download the whole index.
- **A snapshot of the entries rows.** It would go stale with every sync of the index;
  objects do not.
