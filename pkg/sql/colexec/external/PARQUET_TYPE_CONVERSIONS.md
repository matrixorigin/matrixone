# Parquet Type Conversions in MatrixOne

**Audience:** Database Users, Application Developers, Data Engineers

---

## Introduction

MatrixOne supports automatic type conversions when loading Parquet files, allowing flexible data ingestion without manual preprocessing. When the Parquet file's column types don't exactly match your table schema, MatrixOne can automatically convert between compatible types.

---

## Supported Type Conversions

### Integer Type Conversions

#### Widening Conversions

Smaller integer types can be automatically widened to larger ones:

| From (Parquet) | To (Table) | Example |
|----------------|------------|---------|
| INT32 | INT64 (BIGINT) | `123` → `123` |
| UINT32 | UINT64 (BIGINT UNSIGNED) | `456` → `456` |

```sql
-- Parquet file has INT32 column
-- Table expects BIGINT
CREATE TABLE metrics (
    metric_id BIGINT,
    value BIGINT
);

LOAD DATA INFILE {'filepath'='metrics.parq', 'format'='parquet'} 
INTO TABLE metrics;
-- Automatic conversion: INT32 → INT64
```

#### String to Integer Conversions

String representations of integers can be converted to all integer types:

| From (Parquet) | To (Table) | Supported Formats |
|----------------|------------|-------------------|
| STRING | TINYINT (INT8) | `"-128"` to `"127"` |
| STRING | SMALLINT (INT16) | `"-32768"` to `"32767"` |
| STRING | INT (INT32) | `"-2147483648"` to `"2147483647"` |
| STRING | BIGINT (INT64) | Large integers |
| STRING | TINYINT UNSIGNED (UINT8) | `"0"` to `"255"` |
| STRING | SMALLINT UNSIGNED (UINT16) | `"0"` to `"65535"` |
| STRING | INT UNSIGNED (UINT32) | `"0"` to `"4294967295"` |
| STRING | BIGINT UNSIGNED (UINT64) | Large positive integers |

**Accepted String Formats:**
```sql
-- Basic integers
"123"           → 123
"-456"          → -456

-- With whitespace (automatically trimmed)
" 789 "         → 789
"  -321  "      → -321

-- With leading zeros
"00123"         → 123
"0789"          → 789

-- With positive sign
"+123"          → 123
"+456"          → 456
```

**Not Supported:**
```sql
-- Scientific notation
"1e5"           ❌ Error

-- Hexadecimal
"0xFF"          ❌ Error

-- Thousand separators
"1,000"         ❌ Error
```

**Example:**
```sql
CREATE TABLE sales (
    order_id INT,
    quantity SMALLINT,
    customer_id BIGINT
);

-- Parquet file stores all as STRING: "123", "45", "789012"
LOAD DATA INFILE {'filepath'='sales_strings.parq', 'format'='parquet'} 
INTO TABLE sales;
```

---

### Decimal Type Conversions

#### String to Decimal

String representations can be converted to DECIMAL types with automatic precision and scale handling:

| From (Parquet) | To (Table) | Notes |
|----------------|------------|-------|
| STRING | DECIMAL(p, s) where p ≤ 18 | Uses DECIMAL64 internally |
| STRING | DECIMAL(p, s) where p > 18 | Uses DECIMAL128 internally |

**Accepted String Formats:**
```sql
-- Basic decimals
"123.45"                → DECIMAL(10,2)
"-789.12"               → DECIMAL(10,2)

-- Scientific notation (both e and E)
"1e5"                   → 100000
"1E5"                   → 100000
"1.23e-4"               → 0.000123
"1.5E2"                 → 150

-- With positive sign
"+123.45"               → 123.45

-- With whitespace
" 123.45 "              → 123.45

-- With leading zeros
"0123.45"               → 123.45
"00.5"                  → 0.5
```

**Precision and Scale Handling:**
```sql
CREATE TABLE prices (
    product_id INT,
    price DECIMAL(18, 2),    -- 18 total digits, 2 after decimal
    tax_rate DECIMAL(5, 4)   -- 5 total digits, 4 after decimal
);

-- String "123.456" loaded into DECIMAL(18,2) → 123.46 (rounded)
-- String "0.0525" loaded into DECIMAL(5,4) → 0.0525 (exact)
```

**Example:**
```sql
CREATE TABLE financial_data (
    account_id INT,
    balance DECIMAL(20, 4),
    interest_rate DECIMAL(10, 6)
);

-- Parquet strings: "1000000.5", "0.0325"
LOAD DATA INFILE {'filepath'='financial.parq', 'format'='parquet'} 
INTO TABLE financial_data;
```

---

### Floating Point Conversions

#### String to Float/Double

```sql
CREATE TABLE measurements (
    temp_f FLOAT,      -- STRING → FLOAT32
    temp_d DOUBLE      -- STRING → FLOAT64
);
```

**Accepted String Formats:**
```sql
-- Decimal notation
"123.45"               → 123.45
"-789.12"              → -789.12

-- Scientific notation
"1.23e-4"              → 0.000123
"1E10"                 → 10000000000

-- Special values
"Inf"                  → +Infinity
"-Inf"                 → -Infinity
"NaN"                  → NaN

-- With whitespace
" 123.45 "             → 123.45
```

#### Float to Double Widening

FLOAT32 can be automatically widened to FLOAT64:

```sql
-- Parquet file has FLOAT (32-bit)
-- Table expects DOUBLE (64-bit)
CREATE TABLE sensor_data (
    reading DOUBLE
);

LOAD DATA INFILE {'filepath'='sensors.parq', 'format'='parquet'} 
INTO TABLE sensor_data;
-- Automatic widening: FLOAT32 → FLOAT64
```

---

### Date and Time Conversions

#### String to DATE

**Format:** ISO 8601 date format (`YYYY-MM-DD`)

```sql
CREATE TABLE events (
    event_date DATE
);
```

**Accepted Formats:**
```sql
-- Standard ISO format
"2024-01-01"           → 2024-01-01
"1970-01-01"           → 1970-01-01
"2024-12-31"           → 2024-12-31

-- With whitespace (automatically trimmed)
" 2024-01-01 "         → 2024-01-01
```

**Not Supported:**
```sql
-- Other date formats
"01/01/2024"           ❌ Error
"2024.01.01"           ❌ Error
"Jan 1, 2024"          ❌ Error
```

#### String to TIME

**Format:** `HH:MM:SS` or `HH:MM:SS.ffffff`

```sql
CREATE TABLE schedules (
    start_time TIME,         -- Without microseconds
    end_time TIME(6)         -- With microseconds
);
```

**Accepted Formats:**
```sql
-- Basic time format
"12:30:45"             → 12:30:45
"00:00:00"             → 00:00:00
"23:59:59"             → 23:59:59

-- With microseconds
"12:30:45.123456"      → 12:30:45.123456
"00:00:00.000001"      → 00:00:00.000001

-- Empty string (MySQL-compatible behavior)
""                     → 00:00:00
"   "                  → 00:00:00

-- With whitespace
" 12:30:45 "           → 12:30:45
```

**Scale Handling:**
```sql
-- TIME(6) preserves microseconds
"12:30:45.123456" → TIME(6) → 12:30:45.123456

-- TIME(0) truncates microseconds  
"12:30:45.123456" → TIME(0) → 12:30:45
```

#### String to TIMESTAMP

**Format:** `YYYY-MM-DD HH:MM:SS` or `YYYY-MM-DD HH:MM:SS.ffffff`

```sql
CREATE TABLE logs (
    created_at TIMESTAMP,
    updated_at TIMESTAMP(6)
);
```

**Accepted Formats:**
```sql
-- Full timestamp
"2024-01-01 12:30:45"           → 2024-01-01 12:30:45
"1970-01-01 00:00:00"           → 1970-01-01 00:00:00

-- With microseconds
"2024-01-01 12:30:45.123456"    → 2024-01-01 12:30:45.123456

-- Date only (time defaults to 00:00:00)
"2024-01-01"                    → 2024-01-01 00:00:00

-- With whitespace
" 2024-01-01 12:30:45 "         → 2024-01-01 12:30:45
```

**Timezone Handling:**

TIMESTAMP values respect the session's timezone setting:

```sql
-- Set session timezone
SET time_zone = '+08:00';

-- Load data - timestamps interpreted in +08:00 timezone
LOAD DATA INFILE {'filepath'='logs.parq', 'format'='parquet'} 
INTO TABLE logs;
```

---

## Common Usage Patterns

### Pattern 1: Loading CSV-Exported Parquet

Many tools export CSV to Parquet with all columns as strings:

```sql
-- Original CSV had typed data, but Parquet stores as STRING
CREATE TABLE orders (
    order_id INT,
    order_date DATE,
    amount DECIMAL(18,2),
    quantity INT
);

LOAD DATA INFILE {'filepath'='orders_from_csv.parq', 'format'='parquet'} 
INTO TABLE orders;
-- Automatic conversions:
-- STRING → INT, DATE, DECIMAL
```

### Pattern 2: Handling Type Mismatches

```sql
-- Data source uses INT32, your schema uses INT64
CREATE TABLE metrics (
    user_id BIGINT,      -- Your schema: INT64
    count BIGINT
);

-- Parquet file uses INT32
LOAD DATA INFILE {'filepath'='metrics_int32.parq', 'format'='parquet'} 
INTO TABLE metrics;
-- Automatic widening: INT32 → INT64
```

### Pattern 3: Legacy System Migration

```sql
-- Old system stored everything as strings
CREATE TABLE legacy_import (
    id INT,
    created_at TIMESTAMP,
    balance DECIMAL(20,4),
    active TINYINT
);

LOAD DATA INFILE {'filepath'='legacy_data.parq', 'format'='parquet'} 
INTO TABLE legacy_import;
-- Multiple conversions happen automatically
```

---

## Behavior Details

### NULL Value Handling

All conversions properly handle NULL values:

```sql
-- Parquet column is optional (can contain NULLs)
-- Table column is nullable
CREATE TABLE data (
    id INT NOT NULL,
    value INT,              -- Nullable
    description VARCHAR(100)
);

-- NULL in Parquet → NULL in table
-- No conversion attempted for NULL values
```

### Whitespace Handling

All string-based conversions automatically trim leading and trailing whitespace:

```sql
-- Parquet contains: " 123 ", "  456", "789  "
-- Results in: 123, 456, 789
```

This applies to:
- String → Integer conversions
- String → Decimal conversions
- String → Float/Double conversions
- String → Date/Time conversions

### Dictionary Encoding

MatrixOne fully supports Parquet dictionary encoding for all conversions:

- **What it is:** Parquet optimization where repeated values are stored once in a dictionary
- **Benefit:** Faster loading for columns with repeated values
- **Automatic:** No configuration needed

**Example:**
```sql
-- Parquet file uses dictionary encoding for a STRING column
-- with repeated values: "active", "inactive", "active", "inactive"...
-- Dictionary stores: ["active", "inactive"]
-- Rows reference: [0, 1, 0, 1, ...]

-- MatrixOne automatically handles this during conversion
```

### Error Handling

When conversion fails, MatrixOne provides clear error messages:

```sql
-- Parsing error
"abc" → INT
❌ failed to parse 'abc' as int32: invalid syntax

-- Overflow error
"999999" → TINYINT
❌ failed to parse '999999' as int8: value out of range [-128, 127]

-- Invalid date
"2024-13-01" → DATE
❌ failed to parse '2024-13-01' as DATE: invalid argument
```

The load operation stops on the first error, ensuring data consistency.

---

## Limitations

### What's NOT Supported

#### 1. Narrowing Conversions

Conversions that could lose data are not automatic:

```sql
-- INT64 → INT32 ❌ (could overflow)
-- FLOAT64 → FLOAT32 ❌ (could lose precision)
-- DECIMAL(20,2) → DECIMAL(10,2) ❌ (could overflow)
```

**Solution:** Cast explicitly in SQL or preprocess data.

#### 2. Complex String Formats

```sql
-- Hexadecimal integers
"0xFF" → INT ❌

-- Binary notation  
"0b1010" → INT ❌

-- Scientific notation for integers
"1e5" → INT ❌ (use "100000")

-- Thousand separators
"1,000" → INT ❌ (use "1000")
```

#### 3. Non-ISO Date Formats

```sql
-- US format
"01/01/2024" → DATE ❌

-- European format
"01.01.2024" → DATE ❌

-- Month names
"Jan 1, 2024" → DATE ❌

-- Only ISO 8601 format supported:
"2024-01-01" → DATE ✅
```

---

## Best Practices

### 1. Validate Data Quality

When possible, ensure source data quality before loading:

```bash
# Check for invalid formats before loading
# Example: validate all strings are valid integers
```

### 2. Choose Appropriate Types

Match your table types to the expected data range:

```sql
-- If values are always < 32767
CREATE TABLE data (
    value SMALLINT  -- More efficient than INT
);

-- If high precision needed
CREATE TABLE financial (
    amount DECIMAL(20, 4)  -- Better than FLOAT
);
```

### 3. Handle Errors Gracefully

```sql
-- Use transactions for atomicity
START TRANSACTION;

LOAD DATA INFILE {'filepath'='data.parq', 'format'='parquet'} 
INTO TABLE my_table;

-- Check for errors, rollback if needed
COMMIT;
```

### 4. Optimize for Performance

```sql
-- For large files, consider:
-- 1. Ensure Parquet files use dictionary encoding where appropriate
-- 2. Match types exactly when possible (faster than conversion)
-- 3. Use BIGINT for IDs to avoid conversion from INT32
```

---

## Troubleshooting

### Issue: Overflow Errors

**Symptom:**
```
failed to parse '300' as int8: value out of range [-128, 127]
```

**Solutions:**
1. Change table column to larger type (TINYINT → SMALLINT)
2. Filter data in ETL pipeline before loading
3. Check data source for unexpected values

### Issue: Invalid Date Format

**Symptom:**
```
failed to parse '01/01/2024' as DATE: invalid argument
```

**Solutions:**
1. Convert dates to ISO 8601 format before creating Parquet
2. Use ETL tool to transform date strings
3. Ensure source system outputs YYYY-MM-DD format

### Issue: Scientific Notation in Integers

**Symptom:**
```
failed to parse '1e5' as int32: invalid syntax
```

**Solutions:**
1. For DECIMAL: Keep as-is (scientific notation supported)
2. For INT: Convert to plain number "100000"
3. Preprocess Parquet file to expand scientific notation

### Issue: Unexpected NULL Values

**Symptom:**
```
NULL constraint violation
```

**Solutions:**
1. Check if Parquet column is marked as optional
2. Ensure table column allows NULL (remove NOT NULL)
3. Validate source data for unexpected NULLs

---

## Performance Characteristics

### Expected Throughput

Conversion performance depends on several factors:

| Factor | Impact |
|--------|--------|
| Data size | Larger datasets: 1-5M rows/sec |
| Conversion type | Direct types faster than string parsing |
| Dictionary encoding | Significant speedup for repeated values |
| CPU speed | Direct correlation |

### Memory Usage

- **Per-row overhead:** 16-32 bytes for string conversions
- **Vector allocation:** 4-8 bytes per row for result
- **Temporary strings:** Garbage collected automatically

### Optimization Tips

1. **Use dictionary encoding in Parquet** - Faster for columns with repeated values
2. **Match types when possible** - No conversion is fastest
3. **Batch processing** - Parquet pages processed in batches (typically 1000 rows)

---

## Examples by Use Case

### Use Case 1: Data Warehouse ETL

```sql
-- Loading fact table from Parquet export
CREATE TABLE fact_sales (
    sale_id BIGINT,
    sale_date DATE,
    amount DECIMAL(18,2),
    quantity INT,
    customer_id BIGINT
);

-- Source Parquet may have mixed types
LOAD DATA INFILE {'filepath'='sales_export.parq', 'format'='parquet'} 
INTO TABLE fact_sales;
```

### Use Case 2: Log Analysis

```sql
-- Loading application logs
CREATE TABLE app_logs (
    log_id BIGINT,
    timestamp TIMESTAMP(6),
    level TINYINT,
    duration_ms INT,
    user_id BIGINT
);

-- Logs exported with string timestamps
LOAD DATA INFILE {'filepath'='logs_2024.parq', 'format'='parquet'} 
INTO TABLE app_logs;
```

### Use Case 3: Financial Reports

```sql
-- Loading financial data with high precision
CREATE TABLE transactions (
    txn_id BIGINT,
    txn_date DATE,
    amount DECIMAL(20, 4),
    fee DECIMAL(10, 6),
    balance DECIMAL(25, 4)
);

-- Source may have string representations
LOAD DATA INFILE {'filepath'='transactions.parq', 'format'='parquet'} 
INTO TABLE transactions;
```

---

## Summary

MatrixOne's Parquet type conversion system provides:

- **Flexibility:** Load data without strict type matching
- **Simplicity:** Automatic conversions reduce ETL complexity  
- **Safety:** Overflow detection and clear error messages
- **Performance:** Optimized for dictionary encoding and large datasets

**Supported Conversions:**
- ✅ Integer widening (INT32→INT64, UINT32→UINT64)
- ✅ String to all numeric types (integers, decimals, floats)
- ✅ String to date/time types (DATE, TIME, TIMESTAMP)
- ✅ Float widening (FLOAT32→FLOAT64)

For questions or issues, refer to error messages for specific guidance on resolving conversion problems.

