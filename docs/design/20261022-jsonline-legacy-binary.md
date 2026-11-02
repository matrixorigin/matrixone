# Explicit recovery of legacy JSONLINE binary values

Owner: issue #30635. Implementation: `fix/jsonline-setval-scaledown-followups`.
Base: `6cbe375b49696021d72778baa292a66287f080bd`.
Revision: 1, approved for implementation after the design review below.

## Problem and invariant

Older MatrixOne JSONLINE exports encode binary columns as bare Base64 strings.
The current exporter uses `{"$binary":"..."}` and the current reader preserves
bare strings as text. Consequently an old `"AP8="` restores as ASCII instead of
the original bytes `00FF`. Conversely, decoding every Base64-looking string
would corrupt ordinary JSONLINE text such as `"test"`.

The reader must have an explicit input contract: default imports preserve bare
text, while an opted-in legacy import decodes binary-column strings exactly.
The input format cannot be inferred from the value or the destination type.
JSON null remains NULL; decoded bytes equal to `\\N` remain a non-NULL value.

## Decision and alternatives

Add the read option `json_binary='legacy_base64'`, valid only with JSONLINE.
Absence retains all current behavior. The only accepted nonempty value is
`legacy_base64` (case insensitive). It affects BINARY, VARBINARY and BLOB only,
for both object and array records. Wrapped binary values remain readable in
either mode. Malformed legacy payloads fail instead of falling back to text.

Automatic detection is rejected because strings have ambiguous provenance.
A standalone conversion tool avoids a reader option but duplicates streaming,
compression, schema/projection and object-storage handling. An explicit read
option uses the existing common reader without changing the exported format.
This is an opt-in MatrixOne archive compatibility rule, not a JSON standard.

The planner owns option validation; `tree.ExternParam` carries the immutable
choice through existing plan serialization. Infile, S3 and Stage initialization
must enforce the same contract. The shared JSONLINE reader owns decoding.
CREATE/SHOW CREATE must retain the option for read-only external tables.
Writable external tables reject it: their writer already emits the new format.

## Compatibility and lifecycle

No default, existing file, catalog layout or wire schema changes. The raw option
is retained along with the parsed value in the existing JSON parameter payload.
Older readers reject the unknown option during initialization; this opt-in path
requires upgraded readers. Existing tables without the option remain unchanged.
Rollback of a legacy external-table definition requires dropping that option or
converting the archive first, since otherwise a legacy reader would reject it.

Success and error use the existing scan, statement rollback and cleanup owners.
No goroutine, cache, lock, retry, file or long-lived allocation is introduced.
Decoding adds work proportional to selected binary payload bytes and one decoded
buffer, as with the existing wrapper path. Projection, null metadata and binary
width validation keep their existing owners. Non-binary values are unaffected.
No change to file authorization, credentials or tenant boundaries is required.

## Validation

- Planner tests cover infile, S3 and Stage parity, casing, wrong values/formats,
  and raw-option serialization plus SHOW CREATE retention.
- Reader tests cover both shapes, all three binary types, empty values, NUL,
  non-UTF-8, JSON null, decoded `\\N`, current wrappers, malformed Base64,
  non-string legacy values, text controls and the default ambiguous string.
- A minimal public SQL regression loads a checked-in legacy archive and compares
  HEX values, default text behavior, current wrappers and rejection atomicity.
- Owning packages, changed-package static checks and ordinary SQL comparison
  validate the public consumer. No race or scale claim is introduced here.

## Design review before implementation

Trigger: a new public read option affects archive compatibility and serialized
external-table configuration. Reviewed revision: 1 at the base recorded above.
The default and legacy contracts are distinguishable without heuristics; the
planner/reader and CREATE/SHOW CREATE reverse arcs are mapped. Older-reader
rejection, rollback, invalid input, null provenance and bounded decoding are
closed. No new lifetime owner or subsystem is added. The alternatives above
cover the credible smaller choices. Decision: PASS; no open blocking decision.
