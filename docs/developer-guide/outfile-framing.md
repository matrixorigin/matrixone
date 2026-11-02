# OUTFILE framing and external file contracts

`SELECT ... INTO OUTFILE` accepts `FIELDS ESCAPED BY` and `LINES STARTING BY`.
The escape byte defaults to backslash; an explicit empty escape disables it.
The line prefix applies to data records, not the CSV header. With a prefix,
use `HEADER 'false'` if that prefix also occurs in the header.

## CSV compatibility limits

The writer rejects ambiguous framing rather than report a successful export
whose fields or NULLs would change when read with MatrixOne's `LOAD DATA`.
This **deliberately restricts some previously accepted OUTFILE configurations**,
including exports intended only for another CSV consumer. It is not a claim
that all existing OUTFILE syntax continues to produce identical bytes:

- A field delimiter beginning with double quote, CR, LF, NUL or an invalid
  UTF-8 rune is rejected, even if `ESCAPED BY` is absent. For example,
  `FIELDS TERMINATED BY '"|' ENCLOSED BY ''` previously emitted bytes but
  now fails before opening the file. Use a LOAD-compatible delimiter such
  as comma, tab or `|@` instead.
- An enclosure cannot also begin a field/record delimiter or be a recognized
  newline. Ambiguous escape/terminator combinations require an enclosure.
- A line prefix cannot contain its record terminator.
- Some failures depend on values: for example, the literal string `\N` with
  a custom/empty escape, a trailing CR with escaping disabled and LF records,
  or NULL followed by a delimiter which consumes its marker. These fail only
  when the offending value is encountered. Safe values remain exportable.

The NULL check is lazy and shared per statement: field and record boundaries
are checked separately, once each, including when conversion workers run in
parallel. File rotation does not change the framing or repeat the checks.

This is not a universal round-trip guarantee: explicitly disabling escaping
can still emit a value containing an unprotected delimiter, as in MySQL.
Consumers which require the formerly accepted, LOAD-incompatible framing
must transform a supported export outside MatrixOne.

## Parquet text versus binary

Text-family columns without `BINARY_FLAG` use Parquet STRING/UTF8 annotation
and reject invalid UTF-8 bytes before the affected row group is written.
Binary-flagged VARCHAR/VAR_STRING/STRING/TEXT/BLOB-family metadata uses
unannotated BYTE_ARRAY, so arbitrary bytes remain binary for independent
Parquet consumers, not only for MatrixOne's loader. This corrects the previous
UTF8 annotation on BINARY/VARBINARY exports. Existing files are not rewritten.
