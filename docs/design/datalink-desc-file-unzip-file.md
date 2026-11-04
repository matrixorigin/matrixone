# desc_file and unzip_file

Issue: #21825. Implementation: #32180.

## Problem and scope

#21825 asks Datalink for file metadata (`desc_file`) and for reading compressed files
(`unextract_file`). The owner decision on the issue (2026-11-04) fixes the scope:

- `desc_file(path)` describes a file, the entries of a directory, or the files matching a
  pattern: name, size and a directory flag. It has no modification time.
- `unzip_file(path | datalink)` returns the first file of a `.zip` archive as a BLOB, bounded
  like `load_file`. It never writes files.
- A general decompressor (`unextract_file`) and new audio, video and image types are not added.
  PDF and Word datalinks and FULLTEXT on datalink columns already exist.

Both functions are read-only views of storage that SQL can already reach through `stage_list`,
`load_file` and Datalink. Neither adds a storage location, an access path or persistent state.

## Invariants

1. A caller can describe or unzip exactly the files it could already list with `stage_list` or
   read with `load_file` / a Datalink, under the same identity rule (`sql-file-access.md`).
2. `unzip_file` never holds more than the blob limit plus one byte of decompressed output per
   value, and never writes to storage.
3. Every file service and decoder a call opens is closed before the call returns, on success and
   on error. No goroutine, cache or background state outlives the call.
4. `unzip_file` selects the same archive entry as `LOAD DATA ... COMPRESSION 'zip'`, because both
   use one reader (`pkg/common/zipstream`).

## API

### desc_file(path)

A table function with the columns `file varchar`, `size bigint`, `is_dir bool`.

```sql
select file, size, is_dir from desc_file('stage://s/docs/') d;
```

- `path` takes the forms `stage_list` takes (a `stage://` URL, including a pattern). It must be
  a string; any other type is an error. A NULL or empty `path` returns no rows.
- `file` is the path within the stage's storage, as `stage_list` returns it.
- A path ending in `/`, or a path with a wildcard (`*`, `?`, `[`), returns the listing: the
  entries of the directory, or the matching files. The listing is not recursive; each wildcard
  matches within one path segment, as in `stage_list`.
- Any other path is listed first. If the listing is non-empty, it is a directory and its entries
  are returned. Otherwise the path is stat-ed, and a file returns one row.
- A path that names nothing returns no rows. A stat error other than "not found" is returned as
  the statement's error. When the stat finds nothing, the listing's error, if any, is returned.
  A path ending in `/` or with a wildcard returns the listing's error.
- A directory's `size` is 0 and its `is_dir` is true.
- A non-constant `path` (for example a lateral argument) is described per row.
- A column other than `file`, `size` and `is_dir` is an error.

### unzip_file(path) and unzip_file(datalink)

A scalar function returning BLOB, with overloads for `varchar`, `char` and `datalink`. It is
STRICT (NULL in, NULL out) and volatile.

```sql
select unzip_file(cast('stage://s/a.zip' as datalink));
select unzip_file('/data/a.zip');
```

- A string `path` is a file-service path, as `load_file(varchar)` takes. A datalink reads its
  byte range (`offset`, `size`), so a stage URL is read through a datalink.
- The archive is read once, as a stream, from its local file headers. The returned file is the
  first entry, in archive order, that is neither a directory nor hidden (base name starting with
  `.`). This is the LOAD DATA zip rule. Later entries are not read.
- Entries before the returned one are skipped: a deflate entry is decompressed and discarded, a
  stored entry with its size in the local header is skipped by length.
- Supported methods are store and deflate, including zip64 sizes. The entry's CRC-32,
  uncompressed size and compressed size are verified at its end.
- A first file of length 0 returns NULL, as `load_file` returns NULL for an empty file.

Errors, all as statement errors (`invalid input` unless noted):

| Condition | Error |
| --- | --- |
| Not a zip archive | `not a zip archive` |
| No entry that is a file and not hidden | `zip archive contains no file to load` |
| Encrypted entry | `zip entry '<name>' is encrypted` |
| Method other than store or deflate | `... only store and deflate are supported` |
| Stored entry whose size is only in the data descriptor, returned or before the returned entry | `... cannot be read as a stream` / `... cannot be skipped in a stream` |
| CRC-32 or size mismatch | `zip entry '<name>' is corrupt: checksum or size mismatch` |
| Archive ends inside an entry | `zip archive is truncated` |
| Decompressed file larger than the blob limit (64 MB) | internal error `Data too long for blob` |
| File-service, admission or Datalink errors | passed through unchanged |

## Ownership

| Resource | Owner and release |
| --- | --- |
| File-service access | `fileservice.GetForETL` / `GetForETLWithParsedPath` under `process.SQLFileAccessContext`, the same acquisition as `stage_list`, `load_file` and Datalink. A service built for a local path or a path with service arguments belongs to the call and is closed before it returns (`closeOwnedETLFS`, `NewReadCloserWithOwnedFileService`, `Datalink.ownsService`). A configured named service is shared and is not closed. |
| Archive reader and deflate decoder | `unzipContents` closes the decoder and the source reader on every path. |
| Listing | `stageutil.StageListEntriesWithPattern` / `StageStatFile`. The rows are built in the table function's single batch. |
| Zip format | `pkg/common/zipstream`, moved unchanged from `pkg/sql/crt` and shared with LOAD DATA. |

Function id `UNZIP_FILE = 632` is appended after the last allocated id. No existing id changes.

## Admission and security

The file-access contract of `sql-file-access.md` applies unchanged. `desc_file` authorizes its
listing and its stat with `process.AuthorizeSQLFileAccessPath`. `unzip_file` opens through
`SQLFileAccessContext`, like `load_file` and Datalink.

- A CN local path or a server file service requires the `sys` account's `moadmin` role. Any
  other identity gets `only the sys account moadmin role can access SQL file paths`.
- A remote stage or provider uses its own credentials and the stage's status.

`desc_file` exposes names and sizes, which `stage_list` and `load_file` already expose to the same
caller. `unzip_file` returns bytes that the caller could read with `load_file`, decompressed.

## Resource bounds

- **`unzip_file` output:** at most `MaxBlobLen + 1` bytes are read from the decoder per value.
  A larger file fails, so a decompression bomb cannot grow memory beyond the blob limit.
- **`unzip_file` memory per call:** one 64 KB read buffer, one deflate window, and the result
  vector. The result vector holds one value per row, as `load_file` does.
- **`unzip_file` skipped entries:** decompressing an entry before the returned one costs CPU in
  proportion to its decompressed size and holds no memory beyond the read buffer and the window.
  It is bounded by the archive size and the deflate ratio.
- **`desc_file`:** returns the matched entries in one batch. Memory is proportional to the number
  of entries the path matches, as for `stage_list`. A wildcard path lists each directory on the
  pattern's segments once.

## Cancellation

The statement context is passed to every file-service open, and unzip_file checks it before every
read from the archive, whether the entry is skipped or decoded and whatever the file service. A
canceled statement fails the read with the context error and returns no value. Both functions are
synchronous: no goroutine runs after the call. Bytes already in the read buffer are decoded
without checking the context, and that work is bounded by the buffer.

## Alternatives

| Alternative | Decision |
| --- | --- |
| A general `unextract_file` that decompresses to files | Rejected by the owner. It writes server or stage storage, which crosses the file-access contract, and its output has no natural bound. |
| `archive/zip` over the central directory | Rejected. It needs random access and the archive size, so an object-storage source must be read twice or buffered whole. The stream reader reads once, as LOAD DATA does. |
| `unzip_file` as a table function returning every entry | Not built. The owner scope is the first file. Every entry would need a per-entry blob bound and a different entry rule from LOAD DATA. |
| New columns on `stage_list` | Rejected. `stage_list` has a one-column output contract. A separate function keeps it unchanged. |
| A modification-time column | Not built. `fileservice.DirEntry` has no modification time, and the owner scope excludes it. |

## Validation

| Claim | Test |
| --- | --- |
| desc_file: file, directory listing, wildcard, missing path, per-row argument, directory size 0, NULL and empty path, non-string path, unknown column, argument count | `TestDescFile`, BVT `stage/desc_file_unzip_file.sql` |
| desc_file is a registered table function | `TestTableFunctionPrepareDescFile` |
| desc_file admission | `TestDescFileRequiresServerFileAuthority` |
| unzip_file: first loadable entry, constant and per-row paths, datalink, NULL, empty file is NULL, not a zip, missing file, invalid datalink | `TestUnzipFile`, `zipstream.TestNewReader`, BVT |
| unzip_file overloads and BLOB return type | `TestUnzipFileOverloads` |
| unzip_file admission, path and datalink | `TestUnzipFileRequiresServerFileAuthority` |
| unzip_file blob limit | `TestUnzipFileBlobLimit` |
| unzip_file cancellation, before the open and while reading or skipping | `TestUnzipFileCanceled`, `TestUnzipFileCanceledWhileReading` |
| Zip format: deflate and stored entries, entry selection, rejections, CRC and size checks, truncation, zip64 local header, compressed size check | `crt.TestZipDeflateAndStored`, `TestZipEntrySelection`, `TestZipRejections`, `TestZipIntegrity`, `TestZipZip64LocalHeader`, `TestZipCompressedSizeVerified` |
| A stored entry with a data descriptor before the returned entry cannot be skipped | `zipstream.TestNewReader` |
| Function id allocation | `Test_funids` |
