# Frozen temporal SQL contract for PR #28851

Status: target decisions approved by GPT-6-astra/xhigh on 2026-09-26; revised
text pending exact-revision design review.  This is the
versioned C01–C42 snapshot from the PR body at head
`21f8701059cb7fb1dd716df13ab131df930ce958`, amended by the target
decisions below and the integration with current main
`b457977980137e5056be18f03b22da6fc31908e0`, which has reserved MORPC97
for decimal division.  The companion
[design](20260926-temporal-compatibility-rebase.md) states why temporal
contracts now require MORPC98.  The user explicitly accepted C03, C13 and
C12/C34 on 2026-09-26 and delegated the remaining expect decisions to
GPT-6-astra/xhigh.  Implementation and validation remain pending.

## C42. Fresh bootstrap and upgrade

**C42 Startup and upgrade contract:** bootstrap first commits prerequisite tables. Before opening public ingress, each CN reads the bounded trace-view set; existing release views retain their definitions and are not rebound. When a view is absent, the CN waits outside a SQL transaction for an authoritative enabled, non-preparing, admitted, catalog-fenced protocol98 snapshot, then rereads and reconciles all derived views in one bounded transaction per attempt. Catalog authoring depends on that authority, not routing Ready; routing stays closed until completion. Wrong-kind objects and permanent errors fail startup. Known transient transaction errors can retry from a fresh authority snapshot and catalog read; a nonretryable rollback error is never hidden. Restart resumes from actual catalog objects without a new marker, background worker, further protocol epoch or view-specific bypass. All C40 placement/send/receive/persisted checks remain unchanged.

## 1. What consistency means

Correctness is defined by MatrixOne's explicit contract, exact calendar/integer/decimal reasoning, and compatibility obligations. MySQL is comparative evidence. Neither MySQL output nor an existing `.result` file is automatically the oracle.

For the same operation, logical input, declared SQL types and session state, these must agree: constant / expression / column / prepared parameter; scalar / batch; direct SELECT / a SQL consumer; text / actual server-prepared binary protocol; local / remote execution. Compare **value, NULL, result SQL family, FSP/width, error phase/code and warnings**. Changing row order, batch size, constant folding or a prepared handle's preceding execution must not change the result.

Different declared types or explicit operations can have different contracts only where stated below: numeric versus VARCHAR compound grammar; static versus dynamic format typing; explicit conversion versus arithmetic; strict versus permissive assignment; new binding versus a stored 4.2 execution identity. Transport, optimization and an optional diagnostic-output argument are never semantic distinctions. A different function name alone is insufficient justification for different results for equivalent arithmetic.

## 2. Domain, arithmetic and diagnostics

| ID | Required behavior | Reason / exact examples |
|---|---|---|
| C01 Calendar | Ordinary DATE/DATETIME calendar years are 1–9999 with real month/day and leap-year validation. Typed all-zero date is a separate sentinel, not year zero in ordinary arithmetic. TIMESTAMP conversion additionally respects its declared range and session time zone. | No `10000-01-01`, negative seconds in a calendar rendering, integer wrap, or driver-unreadable temporal value may escape. |
| C02 Public duration | TIME is a signed duration, not a time of day. FSP is 0–6. Retain the released endpoint ±838:59:59 (±838:59:59.000000 at FSP6); FSP changes fractional precision, not this endpoint. Negative zero normalizes to zero. | Do not wrap at 24 hours. Wider internal duration operands may be used in checked arithmetic; they are not a license to publish an out-of-domain TIME. |
| C03 Exact arithmetic; revised expectation | DATE_ADD/SUB, ADDTIME/SUBTIME and TIMEDIFF must not return a saturated substitute for an unrepresentable mathematical result. An evaluated arithmetic result outside its declared temporal domain is **row NULL + warning 1441**. Apply this to typed and string arithmetic, both signs, and calendar boundaries. | `ADDTIME(TIME '838:59:59','00:00:01')`, equivalent DATE_ADD, and `TIMEDIFF('838:59:59','-00:00:01')` all become NULL +1441. The former proposal to clip only ADDTIME/SUBTIME/TIMEDIFF is withdrawn: it violates equivalence without a sufficient product reason. |
| C04 Do not saturate operands | Parse/normalize representable internal operands, perform checked arithmetic, then validate the final result. Reject actual internal overflow; do not wrap or clip before a cancellation. TIMESTAMP(date,duration) follows the same composition rule. | `ADDTIME('1234:00:00','-500:00:00') = '734:00:00'`, also with `51 10:00:00` as the first operand. No current-date injection into a duration. |
| C05 NULL / malformed / overflow | In the tolerant temporal functions covered here, SQL NULL propagates without a warning; malformed input produces row NULL without conversion or overflow warning; valid but numerically unrepresentable arithmetic produces NULL +1441. Unsupported SQL types/units remain binding errors. Explicit CAST/assignment and strict numeric PERIOD APIs have their separate rules below. | `NULL + huge_interval` is NULL without an overflow diagnostic. Bad syntax is not numeric zero and is not arithmetic overflow. Adjacent valid rows still return their results. |
| C06 Inactive evaluation | An inactive CASE arm or masked row cannot emit a conversion/overflow warning or an execution error. PREPARE/folding cannot publish an EXECUTE-only diagnostic. Active diagnostics recur correctly on each execution. | `CASE WHEN 0 THEN ADDTIME(TIME '838:59:59','00:00:01') ELSE TIME '00:00:01' END` returns 00:00:01 with no warnings. Active-arm controls must retain their warning. |
| C07 Warning ownership | Each executed failing operation publishes its specified diagnostic once; two distinct evaluated operations may legitimately produce two. Row-dependent evaluation preserves logical diagnostics independent of batch shape. Respect the session's retention limit; do not retain unbounded messages or emit twice at normalization and arithmetic. Unrelated diagnostic contracts, including decimal comparison, remain intact. | Constant-folding is semantically transparent; warning state cannot leak into the next prepared execution. Hard failures must retain their original error and leave cleanup usable. |

This is an intentional arithmetic-policy departure from MySQL's TIME saturation, not an assertion that MySQL returns C03. Conversion/assignment saturation remains explicit below because those operations request conversion into a target domain and have established strict/permissive policies.

## 3. Parsing and intervals

| ID | Required behavior | Examples / boundaries |
|---|---|---|
| C08 Whole-input classification | In duration/calendar arithmetic, classify complete syntax and operand role before validation. A complete `H:MM[:SS][.fraction]` clock remains a duration at all supported hour widths. An invalid unambiguous calendar cannot fall back to a duration. ADDTIME/SUBTIME's second operand is a duration; TIMEDIFF operands must belong to the same family. | `12:34.56` is 12:34:00.56; `01:02.03` is not 2001-02-03; `12:99.56` stays invalid; `24-2-29` is 2024-02-29. |
| C09 Existing calendar spellings | Retain supported separated short years, the 00–69→2000–2069 / 70–99→1970–1999 two-digit window, and legal 8/14-digit compact calendars. Preserve the selected leading-zero duration spellings. Outer whitespace does not change interpretation. | `2024-2-29`, `2024.2.29`, `2024@2@29`, `20240229120000.123456` remain calendar forms; `00000123456` and `1234.5` remain duration forms. Invalid 20240230 remains invalid. Adding a clock preserves the DATE parser's short-year/separator interpretation: `50-01-01 00:00:01` is year2050 and `90-01-01 00:00:01` is year1990. Compact hours24/minutes60/seconds60 are invalid, not normalized into another day. |
| C10 Scalar interval precision | DATE_ADD/SUB scalar INTERVAL MICROSECOND/SECOND/MINUTE/HOUR/DAY numeric amounts retain their original unit, scale to microseconds, then round once half away from zero. Use checked arithmetic and exact DECIMAL handling, including supported DECIMAL256. FLOAT uses its binary value in the established multiplication/rounding path, not its shortest decimal spelling. TIMESTAMPADD retains its declared integer-amount coercion. | ±1.5 HOUR means ±90 minutes for literal/column/marker in DATE_ADD/SUB. Numeric ±1.5 MICROSECOND means ±2 µs in both TIME and DATETIME interval arithmetic. Long fractions must not overflow just because their spelling exceeds int64 digits. |
| C11 Numeric compound normalization | For the six TIME compound units, canonicalize a numeric value before the existing field grammar: remove insignificant fractional zero padding, retain whole-part zeros, expand FLOAT32/64 shortest-round-trip decimal into fixed notation. Do not feed `E` or an exponent sign into the field splitter. | Numeric 1, 1.0 and 1.0000000 are equivalent. Equal exact numeric values must not differ because of DECIMAL scale, column versus constant, or a JDBC setter. Do not claim approximate floating values equal exact decimals at a rounding boundary. |
| C12 Compound grammar compatibility | Retain non-digit separators between numeric fields, left-padding of omitted fields, and the existing singleton fractional-field convention. VARCHAR is field text, not a generic numeric literal; numeric canonicalization does not redefine VARCHAR. | Numeric `1 SECOND_MICROSECOND` is 0.1 s; numeric 1.0 is also 0.1 s; VARCHAR `'1.0' SECOND_MICROSECOND` is 1 s. This unusual rule is preserved compatibility, not a claim of mathematical superiority. A future redesign would need a separate grammar/migration contract. |
| C13 No numeric fields; revised expectation | For newly bound interval arithmetic, a string with **no numeric fields** (`''`, whitespace, `'bad'`) is invalid for scalar and compound units: row NULL, no overflow warning. Same classification for TIME/DATE/DATETIME/TIMESTAMP, ADD/SUB, literal/column/marker, with or without overflow-status reporting. Explicit `0` and `'0'` remain valid zero intervals. | This deliberately changes main/MySQL's permissive compound no-op. Returning an apparently successful unchanged date for arbitrary bad input is rejected. The previous b0 head had TIME=NULL and DATETIME=unchanged; newly bound execution now uses the same invalid-input policy. Existing stored normalized zero is not retroactively reinterpreted. |
| C14 Units | TIME DATE_ADD/SUB supports MICROSECOND, SECOND, MINUTE, HOUR, SECOND_MICROSECOND, MINUTE_MICROSECOND, MINUTE_SECOND, HOUR_MICROSECOND, HOUR_SECOND, HOUR_MINUTE. Calendar and DAY-bearing units fail at binding, including zero/NULL arguments; no normalization bypass. DATE/DATETIME retain calendar/DAY operations. | This is a declared type-domain restriction, not a row-value exception. Existing unsupported TIME `+/- INTERVAL` operator forms do not become supported implicitly. |
| C15 Rounding / resource bounds | Unit scaling precedes final microsecond rounding; carries propagate through all fields and the final date-range check. The entire fraction must be numeric, including discarded rounding digits; a fraction requires digits after its decimal point. Parsing is linear in input length with bounded field storage; no unbounded token list, global numeric-cast change or per-row CAST/TRIM chain. | Cover sign, 6/7/18/38+ fractional digits, very long whole fields, too many fields, outer whitespace, final carry, and cancellation. |

Unit-aware examples for a canonical numeric amount: `1.5 HOUR_SECOND` means 65 seconds; `1.5 HOUR_MINUTE` means 65 minutes. Numeric `1e-14` expands to `0.00000000000001`: the three supported `*_MICROSECOND` units yield 0 µs, MINUTE_SECOND/HOUR_SECOND yield 1 second, HOUR_MINUTE yields 1 minute. These are consequences of the documented compound field grammar; do not replace them with the blanket rule “tiny number means zero in every unit.” Numeric `1e15` gives the preserved singleton 0.1-second interpretation for those microsecond compound units; an overflowing TIME result in the other units follows C03.

## 4. Function values and result types

All result types are fixed by the bound expression, not the first row. Empty/all-NULL results still report the type. FSP must survive execution, CASE/common-type propagation, serialization and re-execution.

| ID / family | Required values / types |
|---|---|
| C16 EXTRACT | Newly bound DATE, TIME, DATETIME, TIMESTAMP and VARCHAR overloads all return signed BIGINT. Compose compound values numerically with fixed field widths and one overall sign: HOUR_SECOND(01:02:03)=10203; HOUR_SECOND(-00:02:03)=-203; SECOND_MICROSECOND(56.123456)=56123456. EXTRACT(WEEK FROM x)=WEEK(x,0). TIMESTAMP uses the execution time zone. Empty/whitespace-only or malformed VARCHAR input is NULL; zero-calendar field extraction observes the same typed-sentinel / execution-mode distinction as the corresponding component function. Numeric ordering, arithmetic, CTAS/view types and negative values must work. Released stored overloads retain their old ABI, below. |
| C17 Raw component extraction | YEAR/MONTH/DAY/DAYOFMONTH preserve accepted partial-zero fields: `'2024-00-15'` gives 2024/0/15. Complete zero-calendar text fields remain execution-mode dependent, while typed zero-sentinel fields remain zero. This does not make a partial-zero value a valid stored calendar. HOUR/MINUTE/SECOND and corresponding EXTRACT fields agree on the same accepted clock grammar. Preserve the clock of a zero calendar; FSP6 carry from `'0000-00-00 23:59:59.9999999'` gives a 24-hour duration clock, not a fabricated date. |
| C18 ADDTIME/SUBTIME | Typed TIME stays TIME; typed DATETIME stays DATETIME; supported typed TIMESTAMP overload keeps its declared type and session conversion. Newly bound string-first results are VARCHAR(29), never today's date for a bare duration. Direct prepared first marker selects TIME(6); explicit VARCHAR casts retain string-family intent. Both signs obey C03/C04. |
| C19 TIMEDIFF | TIME result, maximum known input FSP for typed/static inputs; dynamic string/marker cases use FSP6. Two `.1` literals give TIME(1), not arbitrary TIME(6). Mixed duration/calendar strings are invalid, same-family subtraction is checked and follows C03. |
| C20 DATE_ADD/SUB and TIMESTAMPADD | Preserve DATE only when the bound units/source guarantee an integral date result. TIMESTAMPADD first applies its declared integer-amount coercion; it does not inherit C10 fractional INTERVAL semantics. DATE plus fractional-capable DAY is statically DATETIME(6), even if a current row or parameter happens to be 1.0, 0 or NULL; integer-only DAY remains DATE. Calendar month/year addition clamps a day to the target month's last valid day. Invalid textual calendar arguments return NULL without diagnostics; valid result overflow returns NULL +1441. Validate the resulting calendar and all scaling/negation intermediates. |
| C21 DATEDIFF/TIMESTAMPDIFF | DATEDIFF returns the difference of calendar dates, ignoring clock fields. TIMESTAMPDIFF uses complete elapsed units; YEAR/QUARTER/MONTH take the day and time remainder into account, including Jan31/Feb28/29. Valid representable reversed arguments negate the result; typed/string overloads agree after the same conversion. Zero/invalid/NULL follows the declared tolerant input policy. |
| C22 TIMESTAMP(date,time), MAKETIME | TIMESTAMP pair returns DATETIME with maximum bound FSP (known literals contribute their validated FSP; dynamic string/marker input contributes 6); add the duration without a hidden early saturation and check the final calendar. MAKETIME constructs signed TIME from hour/minute/second; invalid calendar/clock components are NULL without diagnostics, finite fractional seconds round once, and carries are checked. An out-of-domain constructed TIME is NULL +1441 under the revised exact-result policy. Do not alter unrelated general numeric coercion as a shortcut. |
| C23 STR_TO_DATE | Static format binds DATE / TIME / DATETIME from directives; `%d/%e/%D` plus only clock directives counts duration days. `'1 12:34:56'` with `'%d %H:%i:%s'` gives TIME 36:34:56. Date-containing formats validate real calendars. `%f` preserves up to FSP6; day×24 and carries are checked against TIME range. Dynamic format column/marker remains DATETIME(6), including time-only rows that cannot supply a valid calendar: those rows are NULL. Invalid input/format/range returns NULL. Literal `%%` retains the explicit MatrixOne extension, rather than being mistaken for a directive. |
| C24 DATE_FORMAT/TIME_FORMAT | Return VARCHAR, read both value and format for the current row, and propagate NULL from either. Preserve escaped percent and the existing directive grammar. TIME uses duration-aware %H/%k, 12-hour %h/%l/%p and an explicit negative sign; formatting never wraps the underlying duration. Repeated/alternating formats, all-NULL and empty batches cannot reuse a preceding row's formatting state. Empty format retains the established NULL result in both functions. In TIME_FORMAT, numeric date directives use the zero calendar; calendar names and week/year directives with no meaningful TIME value return NULL. Unknown directives retain literal-character handling; no implicit current date. |
| C25 WEEK/YEARWEEK | Explicit modes normalize modulo 8 per row; explicit NULL mode is 0. One-argument WEEK observes default_week_format at EXECUTE; one-argument YEARWEEK uses 0. Mode bits control first weekday, week range and first-week rule; year-boundary and leap-year results must follow the same calendar algorithm. An explicit mode is unaffected by session default changes. Invalid/zero calendar returns NULL. |
| C26 FROM_DAYS/TO_DAYS | FROM_DAYS 0..365 returns typed zero, 366 returns 0001-01-01, 3652424 returns 9999-12-31; all signed values below 366, including negative values, use the established pre-year-one zero sentinel; values above 3652424 return NULL. Check before multiplication to prevent int64 wrap. TO_DAYS/FROM_DAYS round-trip on their common valid ordinary-date domain; the zero sentinel is a separate case. |
| C27 Unix conversions | Keep the existing supported FROM_UNIXTIME epoch interval (0..32536771199 seconds), fractional precision, and session-zone conversion; reject negative/non-finite/out-of-range values without wrapping. Formatted overloads equal formatting the corresponding conversion using each row's format. Signed/unsigned/float/DECIMAL overloads must agree for exactly equivalent representable instants. Do not assume local-time conversions are bijective across a DST overlap. |
| C28 PERIOD_ADD/PERIOD_DIFF | These are strict numeric year-month APIs: preserve the documented two-digit-year window; zero is not a valid year-month period. Validate month 1..12 rather than silently normalizing month13, and reject invalid/negative/unrepresentable periods with the same error across literal/column/marker paths. NULL propagates; checked month-index arithmetic cannot wrap. Valid PERIOD_DIFF is antisymmetric; PERIOD_ADD traverses calendar months. Inactive invalid arguments must not fail a CASE. |
| C29 Conditional and downstream consumers | CASE/COALESCE/GREATEST/LEAST/UNION use the bound common temporal family and sufficient FSP. Comparisons, sort/group/aggregate, INSERT SELECT, CTAS, views and generated/default expressions must consume the actual published physical type. No reinterpretation of a VARCHAR vector as INT64 or a DATETIME vector as TIME. |

Unchanged temporal APIs such as NOW/CURRENT_TIMESTAMP/CURDATE/CURTIME, DATE/TIME aliases, TO_SECONDS and SEC_TO_TIME/TIME_TO_SEC retain their explicit existing contracts unless a shared changed owner affects them. They are included as alias, round-trip, time-zone and shared-parser controls; this PR is not authorization to silently redesign every temporal API. A newly discovered contradiction must be recorded against this contract before changing those expectations.

## 5. Zero dates, conversion and assignment

| ID | Required behavior |
|---|---|
| C30 Execution-time mode | NO_ZERO_DATE controls zero-date SELECT conversions independently of strict assignment mode. With it enabled, DATE/explicit DATE cast of a zero date returns NULL, including DATE(FROM_DAYS(0)); without it, the zero sentinel is preserved. Raw YEAR(FROM_DAYS(0)) stays 0. Mode is resolved at each EXECUTE and sent to remote execution. Folding an enclosing IS NULL must not erase this dependency. |
| C31 Partial zeros | Raw YEAR/MONTH/DAY extraction from an otherwise accepted partial-zero calendar preserves its fields. Calendar conversion and storage validate their own full-date constraints. NO_ZERO_IN_DATE, NO_ZERO_DATE and strict assignment are distinct switches; a policy for one cannot be used as a proxy for another. |
| C32 Explicit TIME conversion | CAST-to-TIME is an explicit conversion boundary: valid out-of-domain duration is clamped to the target TIME endpoint with warning 1292. TIME(expr) and TIME literals retain their existing function grammar; their public results also obey the TIME range but are not universal CAST aliases. Permissive CAST text-prefix conversion is allowed only by the declared text/mode rules and emits 1292; do not trim digits from an invalid minute/second to invent a valid prefix. Entirely malformed permissive CAST input returns NULL with conversion warning. Strict malformed CAST retains its error. This does not authorize clipping inside arithmetic. |
| C33 Assignment | Strict range/invalid assignment rejects the write with the existing assignment error. Non-strict/IGNORE explicitly apply the target's adjustment policy with diagnostics (range 1264, truncation 1265); do not silently reclassify this as SELECT behavior. Same input and mode agree for INSERT, UPDATE, INSERT SELECT and actual prepared execution. NOT NULL/transaction constraints apply after conversion; a failed write cannot be counted as a successful conversion. |
| C34 Empty TIME compatibility exception | Preserve released 4.2 ordinary empty string payload→NULL for CAST-to-TIME and ordinary assignments in both strict/non-strict modes; TIME('') and TIME '' retain zero under their function grammar; whitespace-only→zero TIME. This is a documented release-compatibility exception, not the claim that MySQL behaves this way or that empty is SQL NULL in every grammar. IGNORE explicitly adjusts empty text to zero with warning1265. Literal/column/JDBC setString and ASCII byte payloads must agree within the same SQL source semantics. |
| C35 Source provenance | SQL hex/binary-literal numeric provenance is different from transporting text bytes in a binary protocol. SQL `x''` converts to zero in supported signed/unsigned/float numeric contexts; TIME assignment through its numeric conversion is zero, while direct explicit TIME cast remains the specified NULL. JDBC setBytes(empty) for a string payload follows the empty-payload contract, not SQL-hex provenance. NUL, invalid UTF-8 and non-ASCII digits are not silently stripped into valid temporal text. |

C12 and C34 retain unusual released behavior for compatibility and state exactly where it applies. C03 and C13 instead reject accidental/unreasonable results and are explicitly new target decisions. Do not cite “MySQL does this” as their sole rationale.

The zero-calendar field policy is source- and mode-sensitive.  Default means the
repository default, including `NO_ZERO_DATE` and `NO_ZERO_IN_DATE`:

| Expression/input | `sql_mode=''` | Default mode |
| --- | --- | --- |
| `YEAR/MONTH/DAY/QUARTER(FROM_DAYS(0))` | `0/0/0/0` | `0/0/0/0` |
| Calendar `EXTRACT` from `FROM_DAYS(0)` | zero | zero |
| `YEAR/MONTH/DAY/QUARTER('0000-00-00')` | `0/0/0/0` | all NULL |
| Calendar `EXTRACT` from `'0000-00-00'` | zero | NULL |
| Calendar fields from `'0000-00-00 12:34:56'` | zero | NULL |
| `HOUR/MINUTE/SECOND('0000-00-00 12:34:56')` and matching `EXTRACT` | `12/34/56` | `12/34/56` |
| Clock extraction from `'0000-00-00 23:59:59.9999999'` at FSP6 | `24/0/0` | `24/0/0` |
| `EXTRACT(DAY_SECOND FROM '0000-00-00 12:34:56')` | `123456` | `123456` |
| `YEAR/MONTH/DAY/QUARTER('2024-00-15')` | `2024/0/15/0` | same |
| `EXTRACT(YEAR_MONTH FROM '2024-00-15')` | `202400` | same |
| `EXTRACT(MONTH FROM '2001-02-00')` | `2` | `2` |
| `WEEK(FROM_DAYS(0),0)` and matching `EXTRACT` | NULL | NULL |
| `DATE(FROM_DAYS(0))` / explicit DATE cast | typed zero | NULL |
| `DATE('0000-00-00')` / explicit DATE cast | typed zero | NULL |
| Malformed calendar `'2024-02-30'` in tolerant raw-field extraction | NULL | NULL |

The same input can therefore have raw field value zero while construction of
an ordinary calendar remains invalid.  Clock inspection does not require a
valid calendar.  A typed zero sentinel has already crossed a declared type
boundary; it is not an all-zero text conversion.

Critical conversion and arithmetic boundaries:

| SQL/context | Required outcome |
| --- | --- |
| `TIMESTAMPADD(DAY,5,'2024-02-30')` | NULL, no warning; not a binding error |
| `DATE_ADD('9999-12-31', INTERVAL 1 DAY)` | NULL +1441 |
| `TIMESTAMP('9999-12-31 23:59:59','00:00:01')` | NULL +1441 |
| `MAKETIME(1,60,0)` / `MAKETIME(839,0,0)` | NULL without overflow warning / NULL +1441 |
| `CAST('900:00:00' AS TIME)` | `838:59:59` +1292 |
| Non-strict `CAST('12:34:56tail' AS TIME)` | valid prefix +1292 |
| Non-strict `CAST('12:99:00' AS TIME)` | NULL +1292; no invented valid prefix |
| Strict malformed CAST-to-TIME | evaluation error if active, no inactive CASE error |
| `CAST('' AS TIME)` / ordinary nullable TIME assignment of `''` | NULL without conversion warning |
| `TIME('')` / `TIME ''` | zero under retained function grammar |
| `CAST(X'' AS SIGNED/UNSIGNED/DOUBLE)` / `CAST(X'' AS TIME)` | numeric zero / NULL |
| TIME assignment of `X''` through numeric provenance | zero |
| Invalid timestamp fraction `'2022-01-02 00:00:01.5123-5'` in comparison conversion | conversion rejection; no partial-suffix acceptance |

`TIME 's'` parses through the TIME function rather than CAST.  In
`ADDDATE(TIME '2562047787:00:00', INTERVAL 1 HOUR)`, conversion first clamps
the TIME operand with 1292; the evaluated arithmetic then returns NULL with
1441.  Both diagnostics are required because two distinct operations ran.

## 6. Protocol, prepared lifecycle and 4.2 upgrade

- **C36 Wire/driver:** COM_QUERY and real COM_STMT_EXECUTE publish the same bound SQL contract. Check MySQL field type, length/width, decimals, flags and NULL bitmap as well as value. New EXTRACT is signed BIGINT / JDBC BIGINT; string ADDTIME/SUBTIME is VARCHAR(29); direct first marker is TIME(6); dynamic STR_TO_DATE is DATETIME(6). Driver conversion settings do not redefine server values. For negative or >24-hour TIME, do not use java.sql.Time as the sole oracle. Assert a real ServerPreparedStatement, not emulated preparation.
- **C37 Reuse:** the same prepared handle survives valid→NULL→invalid→valid and string→integer→decimal(scale change)→float→NULL→string where supported. The expression's declared family/FSP remains coherent; NULL bits, diagnostics, parameter physical types and result buffers cannot leak across executions. An explicit source-type change is tested against the conversion contract, not assumed to be byte-for-byte identical.
- **C38 Session:** each EXECUTE observes sql_mode, time_zone and default_week_format. Remote operators receive the same statement snapshot; absent/unavailable required state fails clearly rather than silently falling back to the server default. A read-only fold must not call a diagnostic sink as if an inactive expression executed.
- **C39 Release ABI:** upgrade from **released 4.2**, not an intermediate main revision. Stored EXTRACT IDs0–4 keep VARCHAR/UINT32; new SQL binds appended IDs5–9 BIGINT. Stored string ADDTIME6–8 / SUBTIME6–10 keep DATETIME; newly bound string results use ADDTIME9–11 / SUBTIME11–15. Physical ABI preservation is not permission for old workers to execute a changed new-query value policy. Views rebind stored SQL; stored typed expression identities remain readable. Do not reinterpret an old normalized constant under a new grammar.
- **C40 Placement/admission:** use **one final MORPC98** boundary. Release4.2.0/.1 advertise9 and 4.2.2–.4 advertise10. Every changed new-query contract, including unchanged arithmetic IDs, string/numeric-to-TIME CAST, and SQL HEX/BIT numeric casts with changed grammar/value/diagnostic semantics, must be constrained before placement and rechecked before send/receive/persisted admission. Fall back to a capable local worker or reject; never silently use old behavior. MORPC98 follows main's independent decimal-division MORPC97; no additional intermediate temporal epoch is introduced. A worker's capability does not override a later deployment admission floor.
- **C41 Upgrade/restart:** real release binary→stop→candidate→stop→restart on the same DISK data must preserve ordinary reads/writes/defaults/views. Existing unreleased catalog4.0.8 creates missing view-dependency/refresh tables idempotently before admission; it preserves existing data and propagates lookup/DDL failure. Existing4.2 catalog4.0.6 offset5 must not skip necessary later work. Retry cannot create duplicate metadata or leave startup waiting forever.

## 7. Tests and non-functional acceptance

This is a coverage obligation, not a claim that all combinations have run.

| Axis | Required representative coverage |
|---|---|
| Input / shape | literal, folded expression, explicit cast, column, actual marker; ASCII text and declared binary sources; scalar/constant vector/mixed vector; all-NULL/empty result; both signs, zero, closest valid/invalid boundaries, long whole/fraction fields |
| Numeric / unit | all supported and forbidden TIME units; INT/UINT, FLOAT32/64, DECIMAL64/128/256 where supported, VARCHAR/CHAR/TEXT, NULL; scale0/1/6/7/18/38; independent final-unit arithmetic oracle and nextafter neighbors |
| Calendar / syntax | leap/non-leap century, month end, years1/9999, all-zero/partial-zero, short/compact/separated calendar versus 0/1/2-colon duration, fractions and carries, invalid minute/second and malformed suffix |
| Consumers / errors | direct SELECT, active/inactive CASE, COALESCE/UNION, ordering/arithmetic/grouping, CTAS/view, INSERT/UPDATE/INSERT SELECT; values + result schema + NULL + exact active/inactive diagnostic assertions |
| State / protocol | mode/time-zone/week changes after PREPARE; same-handle recovery; text versus real binary; local versus remote required-version placement/send/receive; old stored expressions and4.2 upgrade/restart |
| Resource / performance | shared parser O(n), bounded temporary fields; no formatter allocation for numeric EXTRACT; benchmark complete typed interval expressions as well as helpers; valid/invalid/long-input batches under CPU/memory pressure; no timeout/retry/sleep-based correctness assertions |

Optimize the existing owner UT tables and temporal BVT instead of duplicating each reproducer into another test file. Use independent expected values, not the implementation helper as its own oracle. A compact property matrix must assert permitted equivalences: ADD(x,d)=SUB(x,-d) within representable domains; paired duration APIs agree; exact numeric literal/column/marker agree; same SQL over text/binary agrees; no active row changes because an adjacent row is invalid. Do not assert false identities across month-end clamping, DST overlaps, source-type grammar differences, or stored-vs-new ABI contracts.

Evidence must distinguish **specified**, **observed failing**, **verified passing**, and **not run**. A BVT green count does not certify this entire contract. Helper zero-allocation results do not prove whole-query CPU/memory performance, and codec/unit tests do not prove a live mixed-binary rollout.

## 8. Field formulas and representative acceptance values

For a calendar use day-of-month D, clock H/M/S and microsecond U; for a duration use D=0 and the full absolute hour count H, then apply its sign once to the final number. Compose fields in base100 and microseconds in base1000000, with checked int64 arithmetic.

| EXTRACT unit | Numeric composition |
|---|---|
| YEAR_MONTH | year×100 + month (calendar only) |
| DAY_HOUR | D×100 + H |
| DAY_MINUTE | (D×100 + H)×100 + M |
| DAY_SECOND | ((D×100 + H)×100 + M)×100 + S |
| DAY_MICROSECOND | DAY_SECOND×1000000 + U |
| HOUR_MINUTE | H×100 + M |
| HOUR_SECOND | (H×100 + M)×100 + S |
| HOUR_MICROSECOND | HOUR_SECOND×1000000 + U |
| MINUTE_SECOND | M×100 + S |
| MINUTE_MICROSECOND | MINUTE_SECOND×1000000 + U |
| SECOND_MICROSECOND | S×1000000 + U |

Single fields return their numeric field; duration-only inputs do not invent a year/month/calendar day. Calendar-only units on a duration are invalid/NULL. WEEK uses mode0. Raw zero-calendar clock extraction follows C17; malformed text does not acquire a fabricated calendar.

| Independent expected witness | Target |
|---|---|
| DATETIME minimum minus 1 second, typed/string and ADD negative/SUB positive | NULL +1441, no malformed date or driver parsing failure |
| DATETIME maximum plus 1 second, typed/string and ADD positive/SUB negative | NULL +1441 |
| TIME endpoint +1 second via DATE_ADD, ADDTIME, SUBTIME(-1s), TIMEDIFF(-1s) | NULL +1441 in each operation; bound result family stays declared |
| Same overflow expression in inactive CASE | ordinary ELSE result, zero warnings |
| `INTERVAL 'bad' HOUR_SECOND`, `INTERVAL '' HOUR_SECOND` on TIME and DATETIME | NULL in all cases, no overflow warning |
| Explicit zero interval / SQL NULL interval | original value / SQL NULL respectively |
| `.1` TIMEDIFF inputs / dynamic markers | TIME(1) / TIME(6), values consistent at declared precision |
| `YEAR(FROM_DAYS(0))` with NO_ZERO_DATE | 0 |
| `DATE(FROM_DAYS(0))` with / without NO_ZERO_DATE | NULL / typed zero |
| `YEAR/MONTH/DAY('2024-00-15')` | 2024 / 0 / 15 |
| Empty ordinary TIME payload in nullable column, strict/non-strict, INSERT/UPDATE, text/binary | NULL; whitespace payload is zero |
| New typed TIME arithmetic plan sent to release4.2 capability9/10 | no old-worker execution; capable fallback or explicit rejection |

Earlier 162-case inventories remain discovery/checklist material, not a frozen oracle: use this contract to update their expected outcomes and statuses. In particular, old clipping expectations and intermediate-version assertions are superseded. Do not copy a historical PASS/FAIL label onto the current head without matching source, configuration and semantics.
