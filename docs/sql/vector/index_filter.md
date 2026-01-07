# Vector Index Filter Pushdown

This document explains filter timing selection for hybrid vector queries with IVF indexes, demonstrating how to optimize queries by pushing down main table filters to the index table.

## Setup

Create a table and index for the examples:

```sql
CREATE TABLE `mini_vector_data` (
    `id` VARCHAR(64) NOT NULL, 
    `text` TEXT DEFAULT NULL, 
    `vec` vecf32(8) DEFAULT NULL COMMENT '8-dim embedding vector', 
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    PRIMARY KEY (`id`)
);

CREATE INDEX `idx_vec` USING ivfflat ON `mini_vector_data` (`vec`) 
    lists = 16 op_type 'vector_l2_ops';
```

Insert 10,000 rows of data.

## Default Behavior (Without Filter Pushdown)

By default, vector queries with `ORDER BY x LIMIT y` use the vector index through `JOIN(scanNode, ivf_search)`. However, the main table filter conditions are not pushed down to the index table, which can result in reading more rows than necessary from the index table.

**Example Query:**

```sql
WITH q AS (
    SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist 
    FROM mini_vector_data
) 
SELECT * FROM q 
WHERE id < 'id5112' AND id > 'id4912' 
ORDER BY dist LIMIT 5;
```

**Simplified Query Plan:**

```
Project
  -> Sort (Limit: 5)
      Sort Key: mo_ivf_alias_0.score
      -> Join (INNER)
          Join Cond: (mini_vector_data.id = mo_ivf_alias_0.pkid)
          -> Table Scan on mini_vector_data
              Filter Cond: (id > 'id4912'), (id < 'id5112')
          -> Table Function on ivf_search (Limit: 25)
              # Internal execution plan for ivf_search:
              Project
                -> Sort (Limit: 25)
                    Sort Key: l2_distance_sq(__mo_index_centroid_fk_entry, [query_vector])
                    -> Project
                        -> Table Scan on __mo_index_secondary_...
                            inputRows=50 outputRows=50
                            Block Sort Key: l2_distance_sq(#[0,1], )
                            Filter Cond: prefix_eq(#[0,2])
                            Block Limit: 25
```

**Key Observations:**
- The index table scan reads **50 rows** from the index table
- After joining with the main table filter, only **1 row** is returned
- This does not meet the `LIMIT 5` requirement, indicating inefficient index usage

## Optimized Behavior (With Filter Pushdown)

For queries with good main table filter selectivity, you can use the `ORDER BY x LIMIT y BY RANK WITH OPTION 'mode=pre'` syntax to push down main table filters to the index table. This changes the execution plan to `JOIN(scanNode, JOIN(ivf_search, secondScan))`, where a bloom filter is generated from the main table filter results and pushed down to filter the index table.

**Example Query:**

```sql
WITH q AS (
    SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist 
    FROM mini_vector_data
) 
SELECT * FROM q 
WHERE id < 'id5112' AND id > 'id4912' 
ORDER BY dist LIMIT 5 BY RANK WITH OPTION 'mode=pre';
```

**Simplified Query Plan:**

```
Project
  -> Sort (Limit: 5)
      Sort Key: mo_ivf_alias_0.score
      -> Join (INNER)
          Join Cond: (mini_vector_data.id = mo_ivf_alias_0.pkid)
          -> Table Scan on mini_vector_data
              Filter Cond: (id > 'id4912'), (id < 'id5112')
          -> Join (INNER)
              Join Cond: (mo_ivf_alias_0.pkid = #[1,0])
              Runtime Filter Build: #[20,0]  # Bloom filter built from main table scan
              -> Table Function on ivf_search (Limit: 25)
                  Runtime Filter Probe: #[19,0]  # Bloom filter applied here
                  # Internal execution plan for ivf_search:
                  Project
                    -> Sort (Limit: 25)
                        Sort Key: l2_distance_sq(__mo_index_centroid_fk_entry, [query_vector])
                        -> Project
                            -> Table Scan on __mo_index_secondary_...
                                inputRows=25 outputRows=25  # Reduced from 50 due to filter pushdown
                                Block Sort Key: l2_distance_sq(#[0,1], )
                                Filter Cond: prefix_eq(#[0,2])
                                Block Limit: 25
              -> Table Scan on mini_vector_data
                  Filter Cond: (id > 'id4912'), (id < 'id5112')
                  # This scan builds the bloom filter that filters the index table
```

**Key Observations:**
- The index table scan reads only **25 rows** (reduced from 50)
- The main table filter is successfully pushed down to the index table using a bloom filter
- The query returns **5 rows**, meeting the `LIMIT 5` requirement
- This demonstrates high recall rate for filter pushdown

## Comparison

| Aspect | Default Mode | With `mode=pre` |
|--------|--------------|-----------------|
| Execution Plan | `JOIN(scanNode, ivf_search)` | `JOIN(scanNode, JOIN(ivf_search, secondScan))` |
| Index Table Rows Read | 50 | 25 |
| Final Result Rows | 1 | 5 |
| Filter Pushdown | No | Yes (via bloom filter) |
| Efficiency | Lower | Higher |

## When to Use `mode=pre`

Use `ORDER BY ... LIMIT ... BY RANK WITH OPTION 'mode=pre'` when:
- Your query has main table filter conditions with good selectivity
- You want to reduce the number of rows read from the index table
- You need to ensure the query returns the correct number of results specified by `LIMIT`

## Summary

The `mode=pre` option enables filter pushdown from the main table to the index table, significantly improving query efficiency by:
1. Reducing the number of rows read from the index table
2. Using bloom filters to efficiently filter index table entries
3. Ensuring queries return the expected number of results when combined with `LIMIT`

