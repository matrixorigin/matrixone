The official Linux x86-64 MatrixOne v4.2.8 binary (SHA-256
`b989ef04364a7d7afbb440030fce46c113388c4a09f351b05f68f0e649a4b936`)
wrote this unaltered version-1 physical archive. The table was created with the
`create_sql` in the manifest, then received `(id,i)` rows `(1,7)`, `(2,0)`,
`(3,NULL)`, and `(4,-1)`. After `mo_ctl('dn','flush','bitcount_int_backup.t')`,
`DUMP TABLE t TO 'stage://intfixture/integer'` created the archive.

The manifest SHA-256 is
`7dad5bf01aa2c474dadc134a463e142e6aad951fa6283757e4ab79575c9f4155`.
The checksum identifies these manifest bytes; the release binary supplied the
historical expression binding.
