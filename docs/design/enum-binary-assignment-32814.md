# 4.2 ENUM assignment identity (#32814)

## Contract

An ENUM stores a member's ordinal, not its label. Assigning an ENUM to a
different ordered definition resolves its visible label in the destination.
A binary `ENUM('A','a')` inserted into binary `ENUM('a','A')` must store
`A` at ordinal 2 and `a` at ordinal 1. A typed NULL and ordinal zero remain
distinct from valid labels, including a declared empty label. Same ordered
members and assignment identity can transfer raw ordinals. DISTINCT still
recovers visible bytes exactly in the source domain before assigning to the
destination.

Use one assignment identity at the existing owners: the native ENUM charset
byte for persisted column types and a separate plan/vector type field for
distributed execution. Identity 0 preserves historical lookups. Tags for
member encoding and assignment identity remain distinct. The legacy and framed
member codecs, native 16-byte type, serialized 20-byte type, and stored
`uint16` ordinal layout remain unchanged. Unsupported authoring and malformed
wire identities reject rather than aliasing to 0. Existing two- and
three-argument expression overloads retain their meanings; new text
assignments use an explicit four-argument overload. Exact supported names are
`utf8mb4_bin` and `utf8mb4_general_ci`.

## DDL provenance

- Explicit supported column COLLATE authors that identity on CREATE or
  ADD/MODIFY/CHANGE. Unsupported column charset/collation declarations reject.
- A supported table COLLATE supplied in the same CREATE is authoritative for
  new ENUM columns in that statement unless a supported column declaration
  overrides it. This records each column's effective identity, not a new
  persisted table-default field. An unsupported explicit table COLLATE is not
  used to author an inherited ENUM.
- The existing 4.2 catalog does not retain a table or database default suitable
  for unqualified ENUM authoring in a later ALTER. Such a declaration keeps
  existing legacy behavior; use a column COLLATE to select a new identity.
  Existing metadata, original CREATE SQL and server settings are not used to
  guess a legacy column's identity. Persisting defaults for subsequent DDL is
  separate work.
- Existing typed reconstruction owns CREATE LIKE, partition rebuild and ALTER
  COPY. Preserve the saved identity, even where the old SQL spelled a COLLATE
  that 4.2 ignored. SHOW CREATE materializes the actual known column identity.
  Reauthoring an ENUM in ALTER routes changed domains through ordinary target
  assignment rather than treating a displayed label as its old ordinal.

## Scope and release gate

This change does not switch codecs, change ENUM comparison operators, enforce
new general-CI duplicate rules, repair old rows, or redesign physical index
keys. Copies of an existing typed index column retain the new metadata; COPY's
key optimization is conservative across changed identities. SHOW FULL COLUMNS and INFORMATION_SCHEMA.COLUMNS expose the effective identity
for typed ENUM columns. The tenant upgrade refreshes existing COLUMNS views.

DUMP/LOAD TABLE v1 accepts legacy ENUM columns only. The manifest's
`create_sql` describes the source DDL and can include declarations ignored by
the release that wrote it. LOAD validates the existing target's catalog
identity before copying any data. For checkpoint exports, generated DDL
preserves the effective collation, and `--no-load` can produce DDL alone.
A restore script with LOAD DATA rejects typed ENUM columns because the CSV
represents ordinals as text, which can also match numeric labels.

CDC compatibility, database/table default persistence, support for restoring
typed ENUM data, and the 4.2-to-main upgrade bridge are separate work.
Do not release a build that authors this metadata before validating the
stopped upgrade bridge and recovery paths. This PR does not support mixed
old/new binaries or metadata downgrade.

Validation compares MySQL-wire `HEX(CAST(e AS CHAR))` and `e+0` for literal,
prepared, INSERT SELECT/DISTINCT, UPDATE, CSV bulk input, explicit and unrelated
ALTER COPY, and restart; it also checks case-insensitive and immutable
identity-zero controls. Focused native/plan/vector tests exercise wire-tag
admission, same-domain shortcuts, Unicode/binary lookup and numeric overflow.
