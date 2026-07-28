# JSON Type Guide

Complete reference for working with JSON data type in MatrixOne database.

---

## Table of Contents

1. [Overview](#overview)
2. [Creating Tables with JSON Columns](#creating-tables-with-json-columns)
3. [Inserting JSON Data](#inserting-json-data)
4. [Querying JSON Data](#querying-json-data)
5. [Modifying JSON Data](#modifying-json-data)
6. [Aggregation and Sorting](#aggregation-and-sorting)
7. [Import and Export](#import-and-export)
8. [JSON Functions Reference](#json-functions-reference)
9. [Best Practices](#best-practices)
10. [Limitations](#limitations)

---

## Overview

MatrixOne supports the `JSON` data type for storing JSON (JavaScript Object Notation) documents. JSON columns can store complex nested structures including objects, arrays, strings, numbers, booleans, and null values.

### Key Features

- ✅ Store complex nested JSON structures
- ✅ Native JSON functions for querying and manipulation
- ✅ Support for arrays and deep nesting
- ✅ Unicode and special character support
- ✅ Efficient storage and retrieval
- ✅ JSONLines format for bulk data loading

### Supported JSON Value Types

| Type | Example | Description |
|------|---------|-------------|
| **Object** | `{"key": "value"}` | Unordered key-value pairs |
| **Array** | `[1, 2, 3]` | Ordered list of values |
| **String** | `"hello"` | Text enclosed in double quotes |
| **Number** | `123`, `45.67` | Integer or floating point |
| **Boolean** | `true`, `false` | Lowercase boolean values |
| **Null** | `null` | Lowercase null value |

---

## Creating Tables with JSON Columns

### Basic Table Definition

```sql
-- Simple table with JSON column
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    specifications JSON
);

-- Table with multiple JSON columns
CREATE TABLE user_profiles (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(100) NOT NULL,
    preferences JSON,
    settings JSON,
    activity_log JSON
);
```

### Column Constraints

JSON columns support limited constraints:

| Constraint | Supported | Notes |
|------------|-----------|-------|
| `NULL` / `NOT NULL` | ✅ Yes | Can specify if column allows NULL values |
| `DEFAULT` | ❌ No | Default values not supported for JSON columns |
| `PRIMARY KEY` | ❌ No | JSON columns cannot be primary keys |
| `FOREIGN KEY` | ❌ No | JSON columns cannot be foreign keys |
| `PARTITION BY` | ❌ No | Cannot partition tables by JSON columns |

```sql
-- Valid: NULL and NOT NULL constraints
CREATE TABLE json_test (
    id INT PRIMARY KEY,
    data JSON NULL,
    required_data JSON NOT NULL
);

-- Invalid: These will fail
CREATE TABLE invalid_table (
    id JSON PRIMARY KEY,                          -- ❌ Error
    data JSON DEFAULT '{"x": 1}',                 -- ❌ Error
    info JSON
) PARTITION BY HASH(info);                        -- ❌ Error
```

---

## Inserting JSON Data

### Basic INSERT Statements

```sql
-- Simple JSON object
INSERT INTO products VALUES (
    1,
    'Laptop',
    '{"brand": "Dell", "ram": 16, "storage": "512GB SSD"}'
);

-- JSON array
INSERT INTO products VALUES (
    2,
    'Product Bundle',
    '["item1", "item2", "item3"]'
);

-- Empty JSON object
INSERT INTO products VALUES (3, 'Empty Config', '{}');

-- Empty JSON array
INSERT INTO products VALUES (4, 'Empty List', '[]');
```

### Multiple Row Insert

```sql
INSERT INTO products (product_id, product_name, specifications) VALUES
    (5, 'Mouse', '{"color": "black", "wireless": true, "dpi": 1600}'),
    (6, 'Keyboard', '{"layout": "US", "switches": "Cherry MX", "backlit": true}'),
    (7, 'Monitor', '{"size": 27, "resolution": "4K", "panel": "IPS"}');
```

### Complex Nested Structures

```sql
-- Deep nesting example
INSERT INTO app_config (config_id, config_data) VALUES (
    1,
    '{
        "pages": [
            "pages/home/home",
            "pages/profile/profile",
            "pages/settings/settings"
        ],
        "window": {
            "background_color": "#ffffff",
            "navigation_bar": {
                "title": "My App",
                "style": "default",
                "background": "white"
            }
        },
        "features": {
            "authentication": {
                "enabled": true,
                "providers": ["email", "oauth"],
                "session_timeout": 3600
            },
            "notifications": {
                "enabled": true,
                "channels": ["push", "email"],
                "preferences": {
                    "marketing": false,
                    "updates": true
                }
            }
        }
    }'
);
```

### Special Characters and Unicode

```sql
-- Unicode and special characters
INSERT INTO test_data (id, data) VALUES
    (1, '{"中文键": "Chinese value", "emoji": "🎉"}'),
    (2, '{"special": "@#$_%^&*()", "quotes": "He said \"Hello\""}'),
    (3, '{"empty_key": "", "key_with_space": "value"}'),
    (4, '{"": "empty key is allowed"}');
```

### Boolean and Null Values

```sql
-- Boolean values (must be lowercase)
INSERT INTO test_data VALUES (5, '{"active": true, "verified": false}');

-- Null values (must be lowercase)
INSERT INTO test_data VALUES (6, '{"optional_field": null}');

-- ❌ Invalid: Uppercase NULL/TRUE/FALSE will fail
-- INSERT INTO test_data VALUES (7, '{"field": NULL}');  -- Error!
```

### Date Strings in JSON

```sql
-- Dates stored as strings in JSON
INSERT INTO events (event_id, event_data) VALUES (
    1,
    '{
        "event_name": "Conference",
        "start_date": "2024-01-15",
        "end_date": "2024-01-17",
        "created_at": "2024-01-01 10:30:00"
    }'
);
```

### Duplicate Keys

JSON objects can have duplicate keys; the last value wins:

```sql
-- Duplicate keys are allowed
INSERT INTO test_data VALUES (
    8,
    '{"x": 1, "x": 2, "x": 3}'
);
-- Stored as: {"x": 3}

INSERT INTO test_data VALUES (
    9,
    '{"name": "first", "name": "second", "name": "final"}'
);
-- Stored as: {"name": "final"}
```

### Invalid JSON

The following will cause errors:

```sql
-- ❌ Incomplete JSON
INSERT INTO test_data VALUES (10, '[1, 2,');           -- Error

-- ❌ Empty string
INSERT INTO test_data VALUES (11, '');                 -- Error

-- ❌ Uppercase NULL
INSERT INTO test_data VALUES (12, '{"key": NULL}');    -- Error

-- ❌ Invalid syntax
INSERT INTO test_data VALUES (13, '{key: "value"}');   -- Error (missing quotes)
```

---

## Querying JSON Data

### Basic SELECT

```sql
-- Query entire JSON column
SELECT product_id, product_name, specifications
FROM products;

-- Filter by NULL/NOT NULL
SELECT * FROM products WHERE specifications IS NOT NULL;
SELECT * FROM products WHERE specifications IS NULL;
```

### JSON Comparison

JSON columns support comparison operators (lexicographic order):

```sql
-- String comparison of JSON values
SELECT * FROM products 
WHERE specifications > '{"brand": "A"}';

-- Equality comparison
SELECT * FROM products 
WHERE specifications = '{"brand": "Dell", "ram": 16}';
```

### JSON_EXTRACT Function

Extract specific values from JSON documents using JSON path expressions.

#### Basic Path Syntax

```sql
-- Extract top-level key
SELECT 
    product_name,
    JSON_EXTRACT(specifications, '$.brand') AS brand
FROM products;

-- Extract nested value
SELECT JSON_EXTRACT(
    '{"user": {"address": {"city": "NYC"}}}',
    '$.user.address.city'
) AS city;
-- Result: "NYC"
```

#### Array Access

```sql
-- Array indexing (0-based)
SELECT JSON_EXTRACT('[1, 2, 3, 4, 5]', '$[0]');  -- Result: 1
SELECT JSON_EXTRACT('[1, 2, 3, 4, 5]', '$[2]');  -- Result: 3
SELECT JSON_EXTRACT('[1, 2, 3, 4, 5]', '$[10]'); -- Result: NULL

-- Extract from object array
SELECT JSON_EXTRACT(
    '[{"name": "Alice"}, {"name": "Bob"}]',
    '$[1].name'
);
-- Result: "Bob"
```

#### Reverse Indexing

```sql
-- Use 'last' keyword for reverse indexing
SELECT JSON_EXTRACT('[1, 2, 3, 4, 5]', '$[last]');      -- Result: 5
SELECT JSON_EXTRACT('[1, 2, 3, 4, 5]', '$[last-1]');    -- Result: 4
SELECT JSON_EXTRACT('[1, 2, 3, 4, 5]', '$[last-2]');    -- Result: 3
```

#### Array Ranges

```sql
-- Extract array ranges
SELECT JSON_EXTRACT('[1, 2, 3, 4, 5]', '$[0 to 2]');         -- [1, 2, 3]
SELECT JSON_EXTRACT('[1, 2, 3, 4, 5]', '$[1 to 3]');         -- [2, 3, 4]
SELECT JSON_EXTRACT('[1, 2, 3, 4, 5]', '$[0 to last]');      -- [1, 2, 3, 4, 5]
SELECT JSON_EXTRACT('[1, 2, 3, 4, 5]', '$[last-2 to last]'); -- [3, 4, 5]
```

#### Wildcard Paths

```sql
-- Extract all array elements
SELECT JSON_EXTRACT('[1, 2, 3]', '$[*]');
-- Result: [1, 2, 3]

-- Extract all object values
SELECT JSON_EXTRACT('{"a": 1, "b": 2, "c": 3}', '$.*');
-- Result: [1, 2, 3]

-- Recursive search for all matching keys
SELECT JSON_EXTRACT(
    '{"a": 1, "b": {"a": 2, "c": {"a": 3}}}',
    '$**.a'
);
-- Result: [1, 2, 3] (all 'a' values at any depth)
```

#### Complex Path Examples

```sql
-- Deep nested extraction
SELECT JSON_EXTRACT(
    '{
        "data": {
            "users": [
                {"name": "Alice", "city": "NYC"},
                {"name": "Bob", "city": "LA"}
            ]
        }
    }',
    '$.data.users[1].city'
);
-- Result: "LA"

-- Multiple level navigation
SELECT JSON_EXTRACT(
    '{"a": [1, "2", {"aa": ["x", "y", {"aaa": "found"}]}]}',
    '$.a[2].aa[2].aaa'
);
-- Result: "found"
```

#### Multiple Path Extraction

```sql
-- Extract multiple paths (returns array)
SELECT JSON_EXTRACT(
    '{"name": "John", "age": 30, "city": "NYC"}',
    '$.name',
    '$.age'
);
-- Result: ["John", 30]

SELECT JSON_EXTRACT(
    '[10, 20, 30, 40]',
    '$[0]',
    '$[2]',
    '$[3]'
);
-- Result: [10, 30, 40]
```

### Type-Specific Extraction Functions

#### JSON_EXTRACT_STRING

Always returns values as strings:

```sql
SELECT JSON_EXTRACT_STRING('{"name": "John", "age": 30}', '$.name');
-- Result: "John"

SELECT JSON_EXTRACT_STRING('{"name": "John", "age": 30}', '$.age');
-- Result: "30" (number converted to string)

SELECT JSON_EXTRACT_STRING('{"active": true}', '$.active');
-- Result: "true" (boolean converted to string)
```

#### JSON_EXTRACT_FLOAT64

Always returns numeric values as floats:

```sql
SELECT JSON_EXTRACT_FLOAT64('{"price": 19.99}', '$.price');
-- Result: 19.99

SELECT JSON_EXTRACT_FLOAT64('{"count": 5}', '$.count');
-- Result: 5.0 (integer converted to float)
```

### Filtering with JSON Paths

```sql
-- Filter by extracted JSON value
SELECT * FROM products
WHERE JSON_EXTRACT(specifications, '$.brand') = 'Dell';

-- Filter with numeric comparison
SELECT * FROM products
WHERE JSON_EXTRACT(specifications, '$.ram') >= 16;

-- Filter with complex conditions
SELECT * FROM products
WHERE JSON_EXTRACT(specifications, '$.brand') = 'Dell'
  AND JSON_EXTRACT(specifications, '$.ram') >= 16
  AND JSON_EXTRACT(specifications, '$.wireless') = true;
```

### UNNEST Function

The `UNNEST` function expands JSON arrays or objects into rows:

```sql
-- Expand JSON array
SELECT * FROM UNNEST('[1, 2, 3, 4, 5]', '$') AS u;

-- Expand JSON object
SELECT * FROM UNNEST('{"a": 1, "b": 2, "c": 3}', '$') AS u;

-- Expand object values
SELECT * FROM UNNEST('{"a": 1, "b": 2, "c": 3}', '$.*') AS u;

-- Expand nested array
SELECT * FROM UNNEST(
    '{"items": [{"id": 1}, {"id": 2}, {"id": 3}]}',
    '$.items'
) AS u;
```

**UNNEST Output Columns:**

| Column | Description |
|--------|-------------|
| `seq` | Sequence number (1, 2, 3, ...) |
| `key` | JSON object key or array index |
| `path` | Full JSON path to the element |
| `index` | Array index (for arrays) |
| `value` | The actual value |
| `this` | Current JSON node |

**Example with table:**

```sql
-- Create table with JSON array
CREATE TABLE events (
    event_id INT PRIMARY KEY,
    participants JSON
);

INSERT INTO events VALUES
    (1, '["Alice", "Bob", "Charlie"]'),
    (2, '["David", "Eve"]');

-- Expand participants
SELECT event_id, u.seq, u.value
FROM events, UNNEST(events.participants, '$') AS u;

-- Result:
-- event_id | seq | value
-- ---------+-----+---------
-- 1        | 1   | "Alice"
-- 1        | 2   | "Bob"
-- 1        | 3   | "Charlie"
-- 2        | 1   | "David"
-- 2        | 2   | "Eve"
```

**Filtering expanded results:**

```sql
SELECT event_id, u.value
FROM events, UNNEST(events.participants, '$') AS u
WHERE u.value LIKE 'A%';
```

---

## Modifying JSON Data

### Replacing Entire JSON Column

```sql
-- Update entire JSON document
UPDATE products
SET specifications = '{"brand": "HP", "ram": 32, "storage": "1TB SSD"}'
WHERE product_id = 1;

-- Update all rows
UPDATE products
SET specifications = '{"updated": true}';
```

### JSON_SET Function

**Purpose**: Insert new keys OR update existing keys (upsert behavior)

**Syntax:**
```sql
JSON_SET(json_column, path, value [, path2, value2, ...])
```

**Examples:**

```sql
-- Update existing key or insert if not exists
UPDATE products
SET specifications = JSON_SET(specifications, '$.ram', 32)
WHERE product_id = 1;

-- Multiple keys at once
UPDATE products
SET specifications = JSON_SET(
    specifications,
    '$.ram', 32,
    '$.storage', '1TB SSD',
    '$.warranty', '3 years'
)
WHERE product_id = 1;

-- Nested path update
UPDATE products
SET specifications = JSON_SET(
    specifications,
    '$.features.connectivity.wifi', '802.11ax'
);

-- Array element update
UPDATE products
SET specifications = JSON_SET(
    specifications,
    '$.tags[0]', 'updated-tag'
);

-- Append to array (use index beyond length)
UPDATE products
SET specifications = JSON_SET(
    specifications,
    '$.tags[10]', 'new-tag'
);
```

**Use in SELECT (without modifying table):**

```sql
-- Preview changes without UPDATE
SELECT 
    product_name,
    specifications AS original,
    JSON_SET(specifications, '$.discount', 10) AS with_discount
FROM products;
```

### JSON_INSERT Function

**Purpose**: Insert new keys ONLY (does not update existing keys)

**Syntax:**
```sql
JSON_INSERT(json_column, path, value [, path2, value2, ...])
```

**Examples:**

```sql
-- Insert only if key doesn't exist
UPDATE products
SET specifications = JSON_INSERT(specifications, '$.warranty', '2 years')
WHERE product_id = 1;
-- If 'warranty' exists: no change
-- If 'warranty' missing: adds "warranty": "2 years"

-- Multiple inserts
UPDATE products
SET specifications = JSON_INSERT(
    specifications,
    '$.warranty', '2 years',
    '$.certified', true,
    '$.notes', null
);

-- Insert nested values
UPDATE products
SET specifications = JSON_INSERT(
    specifications,
    '$.shipping.method', 'express'
);

-- Insert to array (only if index doesn't exist)
UPDATE products
SET specifications = JSON_INSERT(
    specifications,
    '$.features[5]', 'new-feature'
);
```

### JSON_REPLACE Function

**Purpose**: Update existing keys ONLY (does not insert new keys)

**Syntax:**
```sql
JSON_REPLACE(json_column, path, value [, path2, value2, ...])
```

**Examples:**

```sql
-- Replace only if key exists
UPDATE products
SET specifications = JSON_REPLACE(specifications, '$.ram', 32)
WHERE product_id = 1;
-- If 'ram' exists: updates to 32
-- If 'ram' missing: no change

-- Multiple replacements
UPDATE products
SET specifications = JSON_REPLACE(
    specifications,
    '$.ram', 32,
    '$.storage', '1TB SSD'
);
-- Only updates keys that exist

-- Nested replacement
UPDATE products
SET specifications = JSON_REPLACE(
    specifications,
    '$.features.display.brightness', 'high'
);

-- Array element replacement
UPDATE products
SET specifications = JSON_REPLACE(
    specifications,
    '$.tags[0]', 'updated-first-tag'
);
-- Only replaces if tags[0] exists
```

### Function Comparison Table

| Function | Key Exists | Key Doesn't Exist | Use Case |
|----------|------------|-------------------|----------|
| **JSON_SET** | Updates value | Inserts key-value | General-purpose; ensure key exists |
| **JSON_INSERT** | No change | Inserts key-value | Add defaults without overwriting |
| **JSON_REPLACE** | Updates value | No change | Update only existing data |

**Comparison Example:**

```sql
-- Given: {"a": 1, "b": 2}

-- JSON_SET: both operations succeed
SELECT JSON_SET('{"a": 1, "b": 2}', '$.a', 10, '$.c', 3);
-- Result: {"a": 10, "b": 2, "c": 3}

-- JSON_INSERT: only inserts 'c'
SELECT JSON_INSERT('{"a": 1, "b": 2}', '$.a', 10, '$.c', 3);
-- Result: {"a": 1, "b": 2, "c": 3}

-- JSON_REPLACE: only updates 'a'
SELECT JSON_REPLACE('{"a": 1, "b": 2}', '$.a', 10, '$.c', 3);
-- Result: {"a": 10, "b": 2}
```

### DELETE Operations

```sql
-- Delete rows based on JSON value
DELETE FROM products
WHERE JSON_EXTRACT(specifications, '$.discontinued') = true;

-- Delete all with specific category
DELETE FROM products
WHERE JSON_EXTRACT(specifications, '$.category') = 'obsolete';
```

---

## Aggregation and Sorting

### Aggregation Functions

```sql
-- Count non-NULL JSON values
SELECT COUNT(specifications) FROM products;

-- Max JSON value (lexicographic order)
SELECT MAX(specifications) FROM products;

-- Min JSON value (lexicographic order)
SELECT MIN(specifications) FROM products;

-- Count with condition
SELECT COUNT(*) FROM products
WHERE JSON_EXTRACT(specifications, '$.brand') = 'Dell';
```

### GROUP BY with JSON

```sql
-- Group by extracted JSON value
SELECT 
    JSON_EXTRACT(specifications, '$.brand') AS brand,
    COUNT(*) AS product_count
FROM products
GROUP BY JSON_EXTRACT(specifications, '$.brand');

-- Group by JSON column
SELECT 
    specifications,
    COUNT(*) AS count
FROM products
GROUP BY specifications;

-- Complex grouping
SELECT 
    JSON_EXTRACT(specifications, '$.category') AS category,
    JSON_EXTRACT(specifications, '$.brand') AS brand,
    COUNT(*) AS count,
    AVG(JSON_EXTRACT(specifications, '$.price')) AS avg_price
FROM products
GROUP BY 
    JSON_EXTRACT(specifications, '$.category'),
    JSON_EXTRACT(specifications, '$.brand');
```

### ORDER BY with JSON

```sql
-- Sort by JSON column (lexicographic order)
SELECT * FROM products
ORDER BY specifications;

-- Sort by extracted JSON value
SELECT * FROM products
ORDER BY JSON_EXTRACT(specifications, '$.brand');

-- Multiple sort criteria
SELECT * FROM products
ORDER BY 
    JSON_EXTRACT(specifications, '$.category'),
    JSON_EXTRACT(specifications, '$.price') DESC;

-- Sort with NULL handling
SELECT * FROM products
ORDER BY specifications NULLS LAST;
```

**Note on Sorting:**
- JSON sorting uses lexicographic (dictionary) order
- Numbers in JSON are compared as strings unless extracted and cast
- `"10"` < `"2"` in string comparison
- Use `CAST(JSON_EXTRACT(...) AS type)` for numeric sorting

```sql
-- String comparison (incorrect for numbers)
SELECT * FROM products
ORDER BY JSON_EXTRACT(specifications, '$.price');
-- "100" comes before "25"

-- Numeric comparison (correct)
SELECT * FROM products
ORDER BY CAST(JSON_EXTRACT(specifications, '$.price') AS DECIMAL);
-- 25 comes before 100
```

---

## Import and Export

### LOAD DATA from CSV

```sql
-- Create table with JSON column
CREATE TABLE products_import (
    product_id INT,
    product_name VARCHAR(200),
    specifications JSON
);

-- Load from CSV file
LOAD DATA INFILE '/path/to/products.csv'
INTO TABLE products_import
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
```

**CSV file format:**
```csv
product_id,product_name,specifications
1,"Laptop","{""brand"":""Dell"",""ram"":16}"
2,"Mouse","{""color"":""black"",""wireless"":true}"
```

### LOAD DATA from JSONLines

JSONLines format: one JSON object or array per line.

#### Object Format (key-to-column mapping)

```sql
-- Each line: {"column1": value1, "column2": value2, ...}
LOAD DATA INFILE {
    'filepath'='/path/to/data.jsonl',
    'format'='jsonline',
    'jsondata'='object'
} INTO TABLE products;
```

**File format (data.jsonl):**
```json
{"product_id": 1, "product_name": "Laptop", "specifications": {"brand": "Dell"}}
{"product_id": 2, "product_name": "Mouse", "specifications": {"color": "black"}}
```

#### Array Format (position-based mapping)

```sql
-- Each line: [value1, value2, value3, ...]
LOAD DATA INFILE {
    'filepath'='/path/to/data.jsonl',
    'format'='jsonline',
    'jsondata'='array'
} INTO TABLE products;
```

**File format (data.jsonl):**
```json
[1, "Laptop", {"brand": "Dell", "ram": 16}]
[2, "Mouse", {"color": "black", "wireless": true}]
```

#### Compressed Files

```sql
-- Gzip compression
LOAD DATA INFILE {
    'filepath'='/path/to/data.jsonl.gz',
    'format'='jsonline',
    'jsondata'='object',
    'compression'='gzip'
} INTO TABLE products;

-- Bzip2 compression
LOAD DATA INFILE {
    'filepath'='/path/to/data.jsonl.bz2',
    'format'='jsonline',
    'jsondata'='object',
    'compression'='bzip2'
} INTO TABLE products;

-- LZ4 compression
LOAD DATA INFILE {
    'filepath'='/path/to/data.jsonl.lz4',
    'format'='jsonline',
    'jsondata'='object',
    'compression'='lz4'
} INTO TABLE products;

-- Auto-detect compression by file extension
LOAD DATA INFILE {
    'filepath'='/path/to/data.jsonl.gz',
    'jsondata'='object'
} INTO TABLE products;
```

### INTO OUTFILE (Export)

```sql
-- Export to CSV
SELECT product_id, product_name, specifications
FROM products
INTO OUTFILE '/path/to/export.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';

-- Export to TSV
SELECT product_id, product_name, specifications
FROM products
INTO OUTFILE '/path/to/export.tsv'
FIELDS TERMINATED BY '\t';

-- Force quote JSON column
SELECT product_id, product_name, specifications
FROM products
INTO OUTFILE '/path/to/export.csv'
FIELDS ENCLOSED BY '"'
FORCE_QUOTE(specifications);
```

### External Tables

Read data directly from files without loading:

```sql
-- Create external table
CREATE EXTERNAL TABLE products_external (
    product_id INT,
    product_name VARCHAR(200),
    specifications JSON
)
INFILE {
    'filepath'='/path/to/products.csv'
}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Query external table
SELECT * FROM products_external
WHERE JSON_EXTRACT(specifications, '$.brand') = 'Dell';
```

---

## JSON Functions Reference

### JSON_EXTRACT

Extract values from JSON documents.

**Syntax:**
```sql
JSON_EXTRACT(json_doc, path [, path2, ...])
```

**Path Syntax:**

| Pattern | Description | Example |
|---------|-------------|---------|
| `$` | Root element | `$` |
| `.key` | Object member | `$.name` |
| `[n]` | Array element (0-based) | `$[0]`, `$[2]` |
| `[*]` | All array elements | `$[*]` |
| `.*` | All object members | `$.*` |
| `**.key` | Recursive search | `$**.name` |
| `[last]` | Last array element | `$[last]` |
| `[last-n]` | N-th from end | `$[last-1]` |
| `[m to n]` | Array range | `$[0 to 2]` |

**Examples:**

```sql
SELECT JSON_EXTRACT('{"a": 1}', '$.a');                    -- 1
SELECT JSON_EXTRACT('[1, 2, 3]', '$[1]');                  -- 2
SELECT JSON_EXTRACT('{"a": {"b": 1}}', '$.a.b');          -- 1
SELECT JSON_EXTRACT('{"a": [1, 2]}', '$.a[*]');           -- [1, 2]
SELECT JSON_EXTRACT('{"x": {"y": 1}}', '$**.y');          -- [1]
SELECT JSON_EXTRACT('[1, 2, 3]', '$[last]');              -- 3
SELECT JSON_EXTRACT('[1, 2, 3]', '$[0 to 1]');            -- [1, 2]
SELECT JSON_EXTRACT('{"a": 1, "b": 2}', '$.a', '$.b');    -- [1, 2]
```

### JSON_EXTRACT_STRING

Extract value as string type.

**Syntax:**
```sql
JSON_EXTRACT_STRING(json_doc, path)
```

**Examples:**

```sql
SELECT JSON_EXTRACT_STRING('{"name": "John"}', '$.name');     -- "John"
SELECT JSON_EXTRACT_STRING('{"age": 30}', '$.age');           -- "30"
SELECT JSON_EXTRACT_STRING('{"active": true}', '$.active');   -- "true"
```

### JSON_EXTRACT_FLOAT64

Extract numeric value as float.

**Syntax:**
```sql
JSON_EXTRACT_FLOAT64(json_doc, path)
```

**Examples:**

```sql
SELECT JSON_EXTRACT_FLOAT64('{"price": 19.99}', '$.price');  -- 19.99
SELECT JSON_EXTRACT_FLOAT64('{"count": 5}', '$.count');      -- 5.0
```

### JSON_SET

Insert or update JSON values.

**Syntax:**
```sql
JSON_SET(json_doc, path, value [, path2, value2, ...])
```

**Examples:**

```sql
SELECT JSON_SET('{"a": 1}', '$.a', 2);                    -- {"a": 2}
SELECT JSON_SET('{"a": 1}', '$.b', 2);                    -- {"a": 1, "b": 2}
SELECT JSON_SET('{"a": 1}', '$.a', 2, '$.b', 3);          -- {"a": 2, "b": 3}
SELECT JSON_SET('{"a": [1, 2]}', '$.a[2]', 3);            -- {"a": [1, 2, 3]}
```

### JSON_INSERT

Insert JSON values (only if path doesn't exist).

**Syntax:**
```sql
JSON_INSERT(json_doc, path, value [, path2, value2, ...])
```

**Examples:**

```sql
SELECT JSON_INSERT('{"a": 1}', '$.a', 2);                 -- {"a": 1} (no change)
SELECT JSON_INSERT('{"a": 1}', '$.b', 2);                 -- {"a": 1, "b": 2}
SELECT JSON_INSERT('{"a": 1}', '$.a', 2, '$.b', 3);       -- {"a": 1, "b": 3}
```

### JSON_REPLACE

Replace JSON values (only if path exists).

**Syntax:**
```sql
JSON_REPLACE(json_doc, path, value [, path2, value2, ...])
```

**Examples:**

```sql
SELECT JSON_REPLACE('{"a": 1}', '$.a', 2);                -- {"a": 2}
SELECT JSON_REPLACE('{"a": 1}', '$.b', 2);                -- {"a": 1} (no change)
SELECT JSON_REPLACE('{"a": 1, "b": 2}', '$.a', 10);       -- {"a": 10, "b": 2}
```

### JSON_QUOTE

Convert string to JSON string (add quotes and escape).

**Syntax:**
```sql
JSON_QUOTE(string_value)
```

**Examples:**

```sql
SELECT JSON_QUOTE('hello');                    -- "hello"
SELECT JSON_QUOTE('He said "Hi"');             -- "He said \"Hi\""
SELECT JSON_QUOTE('Line1\nLine2');             -- "Line1\\nLine2"
```

### JSON_UNQUOTE

Remove JSON string quotes and unescape.

**Syntax:**
```sql
JSON_UNQUOTE(json_string)
```

**Examples:**

```sql
SELECT JSON_UNQUOTE('"hello"');                -- hello
SELECT JSON_UNQUOTE('"He said \\"Hi\\""');     -- He said "Hi"
SELECT JSON_UNQUOTE('"Line1\\nLine2"');        -- Line1\nLine2
```

### JSON_ROW

Construct JSON object from key-value pairs.

**Syntax:**
```sql
JSON_ROW(key1, value1 [, key2, value2, ...])
```

**Examples:**

```sql
SELECT JSON_ROW('name', 'John', 'age', 30);
-- Result: {"name": "John", "age": 30}

SELECT JSON_ROW('id', 1, 'active', true, 'tags', JSON_ARRAY('a', 'b'));
-- Result: {"id": 1, "active": true, "tags": ["a", "b"]}
```

### UNNEST

Expand JSON arrays or objects into rows.

**Syntax:**
```sql
UNNEST(json_value, [path], [force_array])
```

**Output Columns:**
- `seq`: Sequence number
- `key`: Object key or array index
- `path`: JSON path to element
- `index`: Array index (for arrays)
- `value`: Element value
- `this`: Current JSON node

**Examples:**

```sql
-- Expand array
SELECT * FROM UNNEST('[1, 2, 3]', '$') AS u;

-- Expand object
SELECT * FROM UNNEST('{"a": 1, "b": 2}', '$') AS u;

-- Expand nested path
SELECT * FROM UNNEST('{"items": [1, 2, 3]}', '$.items') AS u;

-- With table
SELECT t.id, u.value
FROM my_table t, UNNEST(t.json_column, '$.items') AS u;
```

---

## Best Practices

### 1. Use Proper JSON Syntax

```sql
-- ✅ Correct: lowercase boolean and null
INSERT INTO test VALUES ('{"active": true, "value": null}');

-- ❌ Wrong: uppercase
-- INSERT INTO test VALUES ('{"active": TRUE, "value": NULL}');
```

### 2. Validate JSON Before Insertion

```sql
-- Test JSON validity in application before INSERT
-- Invalid JSON will cause insertion errors
```

### 3. Use Path Indices for Arrays

```sql
-- ✅ Efficient: Direct index access
SELECT JSON_EXTRACT(data, '$.items[0]') FROM table;

-- ⚠️ Less efficient: Extracting entire array then filtering
SELECT JSON_EXTRACT(data, '$.items[*]') FROM table;
```

### 4. Extract and Index Frequently Queried Fields

```sql
-- Create indexed column for frequently queried JSON field
ALTER TABLE products ADD COLUMN brand VARCHAR(100)
GENERATED ALWAYS AS (JSON_EXTRACT(specifications, '$.brand'));

CREATE INDEX idx_brand ON products(brand);

-- Now queries on brand are faster
SELECT * FROM products WHERE brand = 'Dell';
```

### 5. Cast Extracted Values for Proper Comparison

```sql
-- ✅ Numeric comparison (correct)
SELECT * FROM products
WHERE CAST(JSON_EXTRACT(specifications, '$.price') AS DECIMAL) > 100;

-- ❌ String comparison (incorrect for numbers)
-- SELECT * FROM products
-- WHERE JSON_EXTRACT(specifications, '$.price') > '100';
-- This does lexicographic comparison: "99" > "100"
```

### 6. Use Appropriate JSON Function

```sql
-- Adding optional field: use JSON_INSERT
UPDATE products SET data = JSON_INSERT(data, '$.warranty', '2 years');

-- Ensuring field has value: use JSON_SET
UPDATE products SET data = JSON_SET(data, '$.updated_at', NOW());

-- Updating only existing: use JSON_REPLACE
UPDATE products SET data = JSON_REPLACE(data, '$.price', new_price);
```

### 7. Consider Storage Size

```sql
-- ⚠️ Very large JSON documents may impact performance
-- Consider splitting into multiple columns or tables

-- Instead of:
-- {"field1": "...", "field2": "...", ... "field1000": "..."}

-- Consider:
-- Table 1: Common fields
-- Table 2: Extended attributes (JSON)
```

### 8. Use Temporary Tables for Complex JSON Processing

```sql
-- For complex JSON transformations, use temporary table
CREATE TEMPORARY TABLE temp_expanded AS
SELECT 
    id,
    JSON_EXTRACT(data, '$.field1') AS field1,
    JSON_EXTRACT(data, '$.field2') AS field2
FROM source_table;

-- Process temporary table
SELECT * FROM temp_expanded WHERE field1 = 'value';
```

### 9. Handle NULL vs JSON null

```sql
-- SQL NULL (column is NULL)
INSERT INTO test VALUES (NULL);

-- JSON null (column contains JSON null)
INSERT INTO test VALUES ('null');
INSERT INTO test VALUES ('{"field": null}');

-- Check for SQL NULL
SELECT * FROM test WHERE data IS NULL;

-- Check for JSON null in field
SELECT * FROM test 
WHERE JSON_EXTRACT(data, '$.field') = 'null';
```

### 10. Document JSON Schema

```sql
-- Add comments to document expected JSON structure
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(200),
    -- specifications JSON structure:
    -- {
    --   "brand": string,
    --   "model": string,
    --   "features": [string],
    --   "dimensions": {
    --     "weight": number,
    --     "size": string
    --   }
    -- }
    specifications JSON
);
```

---

## Limitations

### Column Constraints

| Constraint | Status | Notes |
|------------|--------|-------|
| `NULL`/`NOT NULL` | ✅ Supported | Can specify nullability |
| `DEFAULT` | ❌ Not supported | Cannot set default JSON value |
| `PRIMARY KEY` | ❌ Not supported | Use separate INTEGER column |
| `FOREIGN KEY` | ❌ Not supported | Use separate column for relationships |
| `UNIQUE` | ❌ Not supported | Validate in application or use separate column |
| `CHECK` | ❌ Not supported | Validate in application |
| `PARTITION BY` | ❌ Not supported | Partition by different column |

### Invalid JSON

These operations will fail:

```sql
-- ❌ Incomplete JSON
INSERT INTO test VALUES ('[1, 2,');

-- ❌ Empty string
INSERT INTO test VALUES ('');

-- ❌ Uppercase NULL, TRUE, FALSE
INSERT INTO test VALUES ('{"key": NULL}');
INSERT INTO test VALUES ('{"flag": TRUE}');

-- ❌ Missing quotes on keys
INSERT INTO test VALUES ('{key: "value"}');

-- ❌ Single quotes for strings (must use double quotes)
INSERT INTO test VALUES ("{'key': 'value'}");
```

### Sorting Behavior

```sql
-- JSON sorting is lexicographic (dictionary order)
-- Numbers as strings: "10" < "2" < "20"

-- For numeric sorting, extract and cast:
ORDER BY CAST(JSON_EXTRACT(data, '$.count') AS INT)
```

### Size Considerations

```sql
-- Large JSON documents (>1MB) may impact:
-- 1. Query performance
-- 2. Network transfer time
-- 3. Memory usage

-- Recommendation: Keep JSON documents reasonably sized (<100KB optimal)
```

### Duplicate Keys

```sql
-- Duplicate keys are allowed; last value wins
-- {"x": 1, "x": 2, "x": 3} becomes {"x": 3}

-- Be aware of this behavior when:
-- 1. Merging JSON from multiple sources
-- 2. Programmatically constructing JSON
```

### Type Coercion

```sql
-- Extracted values keep their JSON type
-- May need explicit casting for operations

-- Extract returns JSON type
JSON_EXTRACT(data, '$.count')  -- Returns JSON numeric

-- Cast for SQL operations
CAST(JSON_EXTRACT(data, '$.count') AS INT)  -- Returns SQL integer
```

---

## Complete Example

Here's a comprehensive example demonstrating JSON operations in MatrixOne:

```sql
-- ========================================
-- Create Table
-- ========================================
CREATE TABLE products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(200) NOT NULL,
    specifications JSON NOT NULL,
    metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- Insert Data
-- ========================================

-- Insert single product
INSERT INTO products (product_name, specifications, metadata) VALUES (
    'Professional Laptop',
    '{
        "brand": "Dell",
        "model": "XPS 15",
        "category": "Computers",
        "features": ["touchscreen", "backlit keyboard", "fingerprint reader"],
        "hardware": {
            "processor": "Intel i7-12700H",
            "ram": 32,
            "storage": "1TB SSD",
            "graphics": "RTX 3050 Ti"
        },
        "dimensions": {
            "weight": 1.8,
            "size": "15.6 inch",
            "thickness": "18mm"
        },
        "pricing": {
            "msrp": 1899.99,
            "currency": "USD",
            "discount": 0
        }
    }',
    '{
        "in_stock": true,
        "warehouse_location": "US-West",
        "supplier_id": 12345,
        "tags": ["business", "premium", "portable"]
    }'
);

-- Insert multiple products
INSERT INTO products (product_name, specifications, metadata) VALUES
    (
        'Wireless Gaming Mouse',
        '{"brand": "Logitech", "model": "G Pro", "category": "Accessories", "features": ["RGB lighting", "wireless", "programmable"], "hardware": {"sensor": "HERO 25K", "dpi": 25600, "buttons": 8}, "pricing": {"msrp": 129.99, "currency": "USD"}}',
        '{"in_stock": true, "warehouse_location": "US-East", "tags": ["gaming", "wireless"]}'
    ),
    (
        '4K Monitor',
        '{"brand": "LG", "model": "27UP850", "category": "Displays", "features": ["4K", "USB-C", "HDR"], "hardware": {"size": 27, "resolution": "3840x2160", "refresh_rate": 60, "panel": "IPS"}, "pricing": {"msrp": 599.99, "currency": "USD"}}',
        '{"in_stock": false, "warehouse_location": "US-Central", "tags": ["professional", "4k"]}'
    );

-- ========================================
-- Query Data
-- ========================================

-- Select all with extracted fields
SELECT 
    product_id,
    product_name,
    JSON_EXTRACT(specifications, '$.brand') AS brand,
    JSON_EXTRACT(specifications, '$.category') AS category,
    JSON_EXTRACT(specifications, '$.pricing.msrp') AS price
FROM products;

-- Filter by JSON value
SELECT 
    product_name,
    JSON_EXTRACT(specifications, '$.brand') AS brand
FROM products
WHERE JSON_EXTRACT(specifications, '$.category') = 'Computers';

-- Filter by nested value
SELECT 
    product_name,
    JSON_EXTRACT(specifications, '$.hardware.ram') AS ram
FROM products
WHERE CAST(JSON_EXTRACT(specifications, '$.hardware.ram') AS INT) >= 16;

-- Filter by array content (using wildcards)
SELECT 
    product_name,
    JSON_EXTRACT(specifications, '$.features') AS features
FROM products
WHERE JSON_EXTRACT(specifications, '$.features[*]') LIKE '%wireless%';

-- ========================================
-- Update Data
-- ========================================

-- Update entire JSON column
UPDATE products
SET specifications = JSON_SET(
    specifications,
    '$.pricing.discount', 10
)
WHERE JSON_EXTRACT(specifications, '$.category') = 'Computers';

-- Update nested value
UPDATE products
SET specifications = JSON_SET(
    specifications,
    '$.hardware.warranty', '3 years'
)
WHERE JSON_EXTRACT(specifications, '$.pricing.msrp') > 500;

-- Add new field
UPDATE products
SET metadata = JSON_INSERT(
    metadata,
    '$.last_updated', DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s')
);

-- Replace existing field
UPDATE products
SET metadata = JSON_REPLACE(
    metadata,
    '$.in_stock', true
)
WHERE JSON_EXTRACT(metadata, '$.warehouse_location') = 'US-West';

-- ========================================
-- Aggregation and Grouping
-- ========================================

-- Count products by category
SELECT 
    JSON_EXTRACT(specifications, '$.category') AS category,
    COUNT(*) AS product_count
FROM products
GROUP BY JSON_EXTRACT(specifications, '$.category');

-- Average price by brand
SELECT 
    JSON_EXTRACT(specifications, '$.brand') AS brand,
    AVG(CAST(JSON_EXTRACT(specifications, '$.pricing.msrp') AS DECIMAL(10,2))) AS avg_price
FROM products
GROUP BY JSON_EXTRACT(specifications, '$.brand');

-- Products by stock status
SELECT 
    JSON_EXTRACT(metadata, '$.in_stock') AS in_stock,
    COUNT(*) AS count
FROM products
GROUP BY JSON_EXTRACT(metadata, '$.in_stock');

-- ========================================
-- Sorting
-- ========================================

-- Sort by price (numeric)
SELECT 
    product_name,
    JSON_EXTRACT(specifications, '$.pricing.msrp') AS price
FROM products
ORDER BY CAST(JSON_EXTRACT(specifications, '$.pricing.msrp') AS DECIMAL) DESC;

-- Sort by brand and category
SELECT 
    product_name,
    JSON_EXTRACT(specifications, '$.brand') AS brand,
    JSON_EXTRACT(specifications, '$.category') AS category
FROM products
ORDER BY 
    JSON_EXTRACT(specifications, '$.brand'),
    JSON_EXTRACT(specifications, '$.category');

-- ========================================
-- Complex Queries
-- ========================================

-- Find products with specific features
SELECT 
    product_name,
    JSON_EXTRACT(specifications, '$.features') AS features
FROM products
WHERE JSON_EXTRACT(specifications, '$.features[*]') LIKE '%wireless%';

-- Price range query
SELECT 
    product_name,
    JSON_EXTRACT(specifications, '$.pricing.msrp') AS price
FROM products
WHERE CAST(JSON_EXTRACT(specifications, '$.pricing.msrp') AS DECIMAL) BETWEEN 100 AND 1000;

-- Multiple conditions
SELECT 
    product_name,
    JSON_EXTRACT(specifications, '$.brand') AS brand,
    JSON_EXTRACT(specifications, '$.hardware.ram') AS ram
FROM products
WHERE JSON_EXTRACT(specifications, '$.category') = 'Computers'
  AND CAST(JSON_EXTRACT(specifications, '$.hardware.ram') AS INT) >= 32
  AND JSON_EXTRACT(metadata, '$.in_stock') = true;

-- ========================================
-- Expand Array with UNNEST
-- ========================================

-- Expand features array
SELECT 
    p.product_name,
    u.value AS feature
FROM products p,
     UNNEST(p.specifications, '$.features') AS u
WHERE JSON_EXTRACT(p.specifications, '$.category') = 'Computers';

-- ========================================
-- Export Data
-- ========================================

-- Export to CSV
SELECT 
    product_id,
    product_name,
    specifications,
    metadata
FROM products
INTO OUTFILE '/tmp/products_export.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';

-- ========================================
-- Cleanup
-- ========================================

-- DROP TABLE products;
```

---

## Additional Resources

### Related Topics

- MatrixOne Data Types
- Import/Export Operations
- Query Optimization
- Index Strategies

### External References

- [JSON Specification (RFC 8259)](https://tools.ietf.org/html/rfc8259)
- [JSONPath Syntax](https://goessner.net/articles/JsonPath/)
- [MatrixOne Documentation](https://docs.matrixone.io/)

---

**Document Version**: 1.0  
**Last Updated**: 2025-10-29  
**MatrixOne Version**: 1.0+
