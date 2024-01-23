### NOTE:

- Starts with `lib`: `libfaiss_c.dylib` must start with `lib`. This is how you get linked in the cgo.
- CGO library reference starts with `-l`: `#cgo LDFLAGS: -L../../cgo/thirdparty/runtimes/osx-arm64/native -lfaiss_c`
