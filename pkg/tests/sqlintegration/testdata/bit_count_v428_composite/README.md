The official Linux x86-64 MatrixOne v4.2.8 release (commit `10d204b`)
wrote this unedited physical v1 archive with `DUMP TABLE t` after
`mo_ctl('dn', 'flush', 'bitbackup.t')`. The manifest records the CREATE
statement. Inserted `(id,part,d)` rows were `(1,10,3.9)`,
`(1,20,-0.6)`, and `(2,10,NULL)`.

Manifest SHA-256:
`b7c657566e7fc8d7189307c68a226351a0ff9bc7dc4871e9dfb8641fa26456c8`.
