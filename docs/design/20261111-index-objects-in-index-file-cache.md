# Index objects in the index-file cache

## Problem

An IVF-Flat search reads its entries table, about the size of its vectors, on every query.
In the shared disk cache those objects compete with every other table: a scan of another
workload evicts them, and the next search downloads them again, while they take room the
other workload's objects then miss.

## Decision

A CN with an index-file cache keeps the objects of IVF-Flat entries tables whole in it,
as index files beside HNSW and fulltext ones, apart from the shared disk cache.

- **Loading.** `indexfile.OpenObject` opens an object's entry, or reads the object whole
  into the cache and opens it, as `indexfile.Open` does for an index file: one read per
  entry at a time, an object of 16 MiB or more read as two ranges at once while fewer than
  four whole-object downloads, of index objects and of the disk cache, are in flight; each
  complete download is a sample of the download rate the prefetcher sizes its queue by. The IVF-Flat
  search's scan of the entries table opens the objects of its blocks before reading them,
  eight at once, and holds them until the scan ends. A distributed search assigns whole
  objects to CNs by object id, so each CN loads its share. An object that cannot be loaded
  is read the normal way.
- **Reading.** Reads of an entries table carry `IndexFileObject`: its table scans,
  identified by the table type, and the search's own scan. A read with it of an object the
  index-file cache holds is served from there, and the decoded data still enters the
  memory cache. A prefetch with it, through the prefetcher or the IO pipeline, of an
  object the cache holds is not downloaded into the disk cache. Anything else takes the
  normal path.
- **Eviction.** An object's entry is an index file like any other: the cache's LRU evicts
  it, and the next scan that needs it reads it again. A held entry is not evicted, and the
  release of a hold, or a read, marks it used.
- **Consistency.** An entry is the bytes of an immutable object named by the object.
  Which objects and rows a query sees still comes from its snapshot, so results do not
  depend on what the cache holds; only where the bytes come from does.
- **Restart.** After a restart the entries on disk serve reads while the cache loads them.
- **Delete.** Deleting an object through the CN's file service deletes its entry. Objects
  deleted elsewhere, as by TN garbage collection, are no longer read, and their entries
  are evicted.

## Limits

- **The index-file cache is sized for the indexes it serves.** As for HNSW and fulltext
  index files, a CN's index-file cache is assumed to hold the entries objects its searches
  read. When the objects one search holds exceed the cache, each further object is read
  whole and evicted at once, and the search reads its ranges again from object storage.

## Alternatives

- **Make the index-file cache the disk tier of index tables.** The read and prefetch path
  uses the disk cache directly in about forty places.
- **Load in the background and read the disk cache meanwhile.** A read cannot use an
  object until its load ends, so the first scans read their ranges again beside the load.
- **Fill each index whole when it is first used.** A distributed search reads only a
  share of the objects on each CN, which would each read the whole index.
- **A snapshot of the entries rows.** It would go stale with every sync of the index;
  objects do not.
