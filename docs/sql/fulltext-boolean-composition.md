# Indexed MATCH with SQL OR and NOT

A scan-local WHERE predicate can compose indexed MATCH expressions with SQL
AND, OR and NOT, including ordinary scalar predicates and score comparisons.
This is available for FULLTEXT and FULLTEXT2 under their existing experimental
feature settings. For example:

```sql
SELECT id
FROM documents
WHERE MATCH(body) AGAINST('alpha' IN BOOLEAN MODE) OR tag = 1
ORDER BY id;

SELECT id
FROM documents
WHERE NOT MATCH(body) AGAINST('alpha' IN BOOLEAN MODE)
  AND tag IS NOT NULL
ORDER BY id;
```

Every MATCH must use the same table instance and exactly the columns covered
by a valid fulltext index. Nested expressions and repeated MATCH calls are
supported. Several MATCH hits do not duplicate a base row.

Within this composition domain, the relevance is the index's emitted score,
or FLOAT32 zero if the index emits no result for the row. MATCH as a Boolean,
whether as a condition or through CAST(... AS BOOL), is true for every row the
index returns, whatever its score, and false for the others, exactly as a MATCH
alone in WHERE. A term found in every document scores zero but still matches.
Comparisons, numeric casts and projections use the actual score. Indexed all-NULL content without a search result also has score
zero; a non-NULL indexed sibling can still match. Scalar NULL values keep SQL
three-valued logic, so a predicate is retained by WHERE only when it is true.

Projected or ordered copies of a served MATCH use that same score. Missing
scores on real base rows become zero before enclosing user outer joins; a
missing table instance produced by an outer join remains NULL. Explicit
ORDER BY and LIMIT/OFFSET apply to the complete Boolean-filtered result.
Vector ordering uses the exact path rather than candidate-truncating ANN.

This capability does not change standalone projection-only MATCH behavior,
ordinary positive MATCH filtering, relevance algorithms, tokenization, or
FULLTEXT2 asynchronous visibility. FULLTEXT2 searches still observe their
existing visible index generation. Classic NULL/empty patterns retain their
existing invalid-input errors; FULLTEXT2 NULL patterns retain the existing
empty-stream behavior. A required search may report an error even when a
scalar OR branch is true.

Cross-table MATCH arguments, expressions without an exact index, and MATCH
predicates that cannot be owned by a single base scan remain unsupported.
A distinct projection-only MATCH that the Boolean WHERE does not serve also
remains unsupported in this route. Unsupported forms report an error rather
than evaluating only the scalar branch. Complete score streams and base-row
preservation can cost more memory and work than a selective positive MATCH;
normal query resource limits still apply.
