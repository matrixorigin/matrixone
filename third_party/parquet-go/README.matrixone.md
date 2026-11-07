# parquet-go local module

This directory contains production Go, assembly and header files from
`github.com/parquet-go/parquet-go` v0.25.1 (module checksum
`h1:l7jJwNM0xrk0cnIIptWMtnSnuxRkwq53S+Po3KG8Xgo=`), its module files and
licenses, plus the upstream thrift tests. Upstream test data and unrelated
upstream tests are omitted. The only production modification is in
`encoding/thrift/decode.go`: skip coalesced boolean values only in struct field
context, leaving collection values to the ordinary skip path.

The repository's main module replaces parquet-go with this directory. Go does
not propagate a dependency's replace directives to downstream main modules;
external consumers must provide their own replacement to obtain the fix.
The replace also prevents automatic library-version discovery in CreatedBy.

When updating, verify the downloaded module checksum and compare the complete
production file list and contents against the chosen upstream release, retain
licenses and platform-specific files, reapply or retire the patch, and run both
the nested thrift tests and MatrixOne's thrift, reader and writer compatibility
regressions. Retire this copy only when a verified distributable version has
both correct field/collection decoding and compatible page statistics behavior.
