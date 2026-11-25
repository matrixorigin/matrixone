MatrixOne built from PR merge base `dea4f40b17d0893da6823fec1fdd27cf6493dea3`
(binary SHA-256 `fb462766eb341ca68d0e4fe23ef1cf4d28022defad7275a9811315464be8bd17`)
wrote this unaltered version-1 physical archive. The table was created with the
`create_sql` in the manifest, then received `(id,i)` rows `(1,7)`, `(2,0)`,
`(3,NULL)`, and `(4,-1)`. After `mo_ctl('dn','flush','bitcount_int_base.t')`,
`DUMP TABLE t TO 'stage://intbase/integer'` created the archive.

The manifest SHA-256 is
`0f5398ad632f67555c04962965629eb873ab03b71dcac5a00895cca8a5062730`.
No release annotation is needed to load this schema-matching v1 archive.
