The official Linux arm64 MatrixOne v4.2.8 release (`10d204b`) wrote this
unaltered v1 archive with `DUMP TABLE t TO '/work/released-v1'`.
The table was created with the `create_sql` in the manifest. It received
`(id,d,f)` rows `(1,3.9,3.9)`, `(2,-0.6,-0.6)`, `(3,NULL,NULL)`, and
`(4,9223372036854775808,3.5)`, then `mo_ctl('dn', 'flush', 'bitbackup.t')`.

The manifest SHA-256 is
`3217cdc85ac5c07d7d736754945006b6a35295303ed5dfcdbc24af994d4e033b`.
The checksum identifies these manifest bytes. The archive's generation on the
release supplies the expression provenance for the test.
