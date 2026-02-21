# MatrixOne NULL-Safe Equal Operator (`<=>`) Implementation Design Document

## 1. Requirement Analysis

**Feature Request:** [Issue #23009](https://github.com/matrixorigin/matrixone/issues/23009)

**Core Requirement:**
Implement the MySQL-compatible NULL-Safe Equal operator `<=>`. This operator compares two expressions and returns `1` (TRUE) if both are equal or both are NULL; otherwise, it returns `0` (FALSE).

**Behavior Comparison:**
| Expression A | Expression B | A = B | A <=> B |
| :--- | :--- | :--- | :--- |
| `1` | `1` | `1` | `1` |
| `1` | `0` | `0` | `0` |
| `1` | `NULL` | `NULL` | `0` |
| `NULL` | `NULL` | `NULL` | `1` |

---

## 2. Implementation Design

### 2.1 Architectural Hierarchy

The implementation covers both the Plan layer and the Function Execution layer of the SQL engine:
1.  **Plan Layer:** Responsible for AST parsing, Tuple expansion, operator binding, and property deduction.
2.  **Function Layer:** Provides vectorized implementation of the NULL-Safe comparison logic.

### 2.2 Detailed Design and Changes

#### 2.2.1 Plan Layer Support
*   **Tuple Expansion:** In `pkg/sql/plan/base_binder.go`, logic has been implemented to handle Tuple expansion for `tree.NULL_SAFE_EQUAL`.
    *   Semantics: `(a, b) <=> (c, d)` is transformed into equivalent logic `(a <=> c) AND (b <=> d)`.
    *   Alignment: This behavior is compatible with MySQL and aligns with MO's existing handling logic for the `EQUAL` (`=`) operator.
*   **Optimizer Properties (NotNullable):** 
    *   In `pkg/sql/plan/function/list_operator.go`, `<=>` is marked with `plan.Function_PRODUCE_NO_NULL`.
    *   Effect: The optimizer can deduce that the result of this expression is never NULL, enabling more effective `NOT` pushdown, equivalence deduction, and `IS NULL` constant folding.
*   **Limitations (Hash Join & Zonemap):**
    *   **Join:** `<=>` is NOT treated as an equi-join condition ( `IsEqualFunc` is not modified). This is because the underlying Hash Join operator currently may not correctly handle NULL key matching. Enabling it prematurely would result in NULL rows being dropped. Currently, `<=>` executes via Nested Loop Join or Cross Product to ensure correctness.
    *   **Zonemap:** `Function_ZONEMAPPABLE` is NOT marked. The default Zonemap evaluation logic is based on Min/Max and ignores NULL value statistics, which could lead to blocks containing NULL values being incorrectly filtered out.

#### 2.2.2 Execution Layer (Function) Implementation
*   **Non-Strict Mode:** `<=>` does not use the `plan.Function_STRICT` flag because it needs to handle NULL inputs rather than propagating them directly.
*   **Vectorized Implementation:** A new `nullSafeEqualFn` has been added in `pkg/sql/plan/function/func_compare.go`.
    *   **Generic Handling:** Uses `opBinaryFixedFixedToFixedNullSafe` for fixed-length types and `opBinaryBytesBytesToFixedNullSafe` for variable-length types.
    *   **NULL Handling Logic:**
        *   `NULL <=> NULL` -> `1` (True)
        *   `Value <=> NULL` -> `0` (False)
        *   `Value <=> Value` -> Normal equality comparison logic.
    *   **Multi-Type Support:** Core comparison logic covers all major data types supported by MatrixOne, including `BOOL`, `INT`, `FLOAT`, `DECIMAL`, `CHAR/VARCHAR`, `JSON`, `DATE/TIME/TIMESTAMP`, `UUID`, and `ARRAY`.
    *   **Result Reset:** Explicitly resets the NULL mask of the result vector to ensure the output is always valid (non-NULL).

#### 2.2.3 Metadata Registration
*   Defined `NULL_SAFE_EQUAL` (406) in `pkg/sql/plan/function/function_id.go` and updated tests.
*   Exported relevant variables in `pkg/sql/plan/function/init.go` for external reference.

---

## 3. Testing Plan

### 3.1 Coverage Scenarios
A new BVT test case `test/distributed/cases/function/func_null_safe_equal.sql` has been added, comprehensively covering the following scenarios:
1.  **Basic Scalar Comparison:** Covering all NULL combinations (`1<=>1`, `1<=>NULL`, `NULL<=>NULL`, etc.).
2.  **Table Data Comparison:** Behavior of `<=>` with Numeric, String, and Boolean types in tables.
3.  **Complex Type Comparison:**
    *   **JSON:** Supports NULL-safe comparison and Join between JSON columns.
    *   **Decimal:** Validates alignment and NULL comparison across different Precision/Scale.
    *   **Date/Time/Timestamp:** Validates matching of Date/Time types under various NULL conditions.
4.  **JOIN Association:** Verifies that when `<=>` is used as a Join condition, NULLs can correctly match NULLs (verifying correctness over performance).
5.  **Implicit Conversion:** Validates `1.0 <=> 1` and mixed scenarios with numbers and strings.
6.  **Tuple Support:** Verifies the expansion logic for `(a, b) <=> (c, d)` and scenarios involving complex types like JSON.
7.  **Optimizer Property Verification:** Verifies that `(val <=> NULL) IS NULL` is correctly optimized to `0` (False).
8.  **Constant Folding:** Verifies that `WHERE NULL <=> NULL` correctly returns rows.

### 3.2 Verification Results
All test cases have been run and verified against a live MatrixOne environment, with results completely consistent with MySQL behavior.

---

## 4. Performance Implications and Limitations

### 4.1 Why Hash Join is Not Supported
Currently, MatrixOne's Hash Join operator (based on `HasMap`) defaults to ignoring NULL keys during the build and probe phases (adhering to standard SQL `=` semantics).
Forcing the enablement of the Hash Join path for `<=>` would cause rows with `NULL <=> NULL` to be directly discarded during the probe phase due to "key is NULL", resulting in incorrect results.
Therefore, the current implementation falls back to Nested Loop Join or Cross Product + Filter to ensure result correctness.

### 4.2 Why Zonemap is Not Supported
The default Zonemap evaluation logic is primarily based on `[Min, Max]` range coverage. For queries like `col <=> NULL`, checking whether `NullCount > 0` in the Block is required.
The generic evaluator has not yet implemented this specialized logic for `<=>`. Directly reusing the generic logic might fail comparison between `Min/Max` and NULL, leading to incorrect filtering of Blocks containing NULL data.
Therefore, the `Function_ZONEMAPPABLE` marker has been removed to force full scanning (or fallback to other indexes) to prevent data loss.

---

## 5. List of Modified Files

| File Path | Description of Responsibility |
| :--- | :--- |
| `pkg/sql/plan/function/function_id.go` | Define ID and register operator name. |
| `pkg/sql/plan/function/function_id_test.go` | Update ID test mapping. |
| `pkg/sql/plan/function/init.go` | Initialize global reference variables. |
| `pkg/sql/plan/function/list_operator.go` | Register function overloads and mark `PRODUCE_NO_NULL`. |
| `pkg/sql/plan/base_binder.go` | Implement Tuple expansion logic. |
| `pkg/sql/plan/function/func_compare.go` | Implement core comparison logic. |
| `test/distributed/cases/function/func_null_safe_equal.sql` | Test case set. |
| `test/distributed/cases/function/func_null_safe_equal.result` | Expected execution results. |

---

## 6. Future Optimization Directions (Open Issues)

1.  **Support Hash Join:** Refactor the Hash Join operator (`HasMap` and Join Probe logic) to support and correctly handle equality matching for NULL keys.
2.  **Support Zonemap:** Implement specialized Zonemap evaluation logic for `<=>` to utilize `NullCount` information in ZoneMap for coarse-grained filtering.