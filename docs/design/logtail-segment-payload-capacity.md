# Logtail segment payload capacity

Related issue: https://github.com/matrixorigin/matrixone/issues/29562.
Implementation: the CI UT racing optimization PR containing this document.

## Owner and measured problem

The logtail server's existing segment pool allocates a maximum-message-sized
payload for every new segment. The issues allocation profile attributes about
1.15 GB to this pool constructor. `morpcStream.write` already knows the exact
chunk length before it acquires a segment; only that length is sent. Capacity
for the largest possible message is not required for each small response.

## Contract

Create an empty segment in the existing pool. In the existing writer, grow its
payload only when current capacity cannot hold the actual chunk. Allocate
`min(chunkLimit, max(chunkLength, 2*oldCapacity))` bytes, then set the payload
length to the chunk length and copy the complete chunk. This gives bounded
geometric growth without copying bytes that will immediately be overwritten.
Capacity never exceeds the existing valid chunk limit. A cold full-size chunk
still requires one allocation and one copy, as before.

Keep the original serialized response buffer, Split, header limit calculation,
sequence numbering, message size, wire format and codec unchanged. Each segment
owns a separate payload; it never aliases the temporary serialized buffer or
another queued segment. Keep the existing transport ownership transfer and
release callback. Release resets metadata while retaining this segment's
payload capacity. No new pool, size class, cache, worker or lifetime state.

The only production segment Acquire consumer is the writer. The pool API still
returns an owned segment, but callers must not assume a maximum-sized initial
payload. Existing test-only consumers are adjusted to exercise the actual
serialization limit rather than depend on eager allocation.

## Validation and tradeoffs

Preserve the response-size test's maximum header and exact wire-size boundary.
Check cold empty payload, small/large/small reuse and reset metadata. Exercise
the writer with exact-limit and multi-segment responses through the existing
capturing transport fixture; reassemble and decode the bytes independently,
verify order/IDs/lengths and no mutation of earlier queued segments. Retain
existing cancellation, failed write, session close and real transport tests.

Measure cold and warm small and full-size writes, plus increasing payload sizes,
in normal and race modes. Check the whole logtail service and real logtail
consumer closure. Then measure cumulative issues CPU, allocation, RSS and time.
Gradually increasing payload sizes can cause extra growth allocations relative
to preallocating the maximum immediately; geometric growth bounds this cost and
must be measured rather than assumed harmless. No claimed CI-minute saving
until actual CI evidence.
