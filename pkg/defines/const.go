// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package defines

import "math"

// Header information.
const (
	OKHeader          byte = 0x00
	ErrHeader         byte = 0xff
	EOFHeader         byte = 0xfe
	LocalInFileHeader byte = 0xfb
)

const (
	SharedFileServiceName  = "SHARED"
	LocalFileServiceName   = "LOCAL"
	ETLFileServiceName     = "ETL"
	StandbyFileServiceName = "STANDBY"
	TmpFileServiceName     = "TMP"
	// sub fileservices
	SpillFileServiceName = "__spill"
)

const (
	MORPCMinVersion    int64 = math.MinInt64
	MORPCVersion1      int64 = 1
	MORPCVersion2      int64 = 2
	MORPCVersion3      int64 = 3  // start from 1.3.0
	MORPCVersion4      int64 = 4  // start from 2.0.1
	MORPCVersion5      int64 = 5  // assignment-aware CHAR/VARCHAR casts
	MORPCVersion6      int64 = 6  // ordered aggregate pipeline configuration
	MORPCVersion7      int64 = 7  // structured CHECK constraint metadata and enforcement
	MORPCVersion8      int64 = 8  // versioned exact runtime-filter key contract
	MORPCVersion9      int64 = 9  // AUTO_INCREMENT epoch-fenced commit
	MORPCVersion10     int64 = 10 // bounded Sorted64 membership-filter wire format
	MORPCVersion11     int64 = 11 // persisted appendable-object abort metadata
	MORPCVersion12     int64 = 12 // prepared-parameter provenance in remote process metadata and aggregate trailers
	MORPCVersion13     int64 = 13 // lossless v2 prefix-index metadata
	MORPCVersion14     int64 = 14 // utf8mb4 text MIN/MAX collation semantics
	MORPCVersion15     int64 = 15 // CHECK metadata in rename-column alter requests
	MORPCVersion16     int64 = 16 // information_schema CHECK_CONSTRAINTS table function
	MORPCVersion17     int64 = 17 // ordered-set percentile aggregate IDs
	MORPCVersion18     int64 = 18 // prepared-parameter binary-string metadata
	MORPCVersion19     int64 = 19 // remote bounded partition Top-N operator
	MORPCVersion20     int64 = 20 // target-aware multi-table UPDATE pipeline fields
	MORPCVersion21     int64 = 21 // lookup-only RIGHT DEDUP for proven-unique insert input
	MORPCVersion22     int64 = 22 // typed user-defined variable migration
	MORPCVersion23     int64 = 23 // explicit-text runtime string provenance
	MORPCVersion24     int64 = 24 // per-target affected-row selectors for repeated physical UPDATE targets
	MORPCVersion25     int64 = 25 // UPDATE changed-row counting
	MORPCVersion26     int64 = 26 // statement LAST_INSERT_ID in remote terminal results
	MORPCVersion27     int64 = 27 // native ASOF join pipeline fields and semantics
	MORPCVersion28     int64 = 28 // owner-local lock snapshots and table-scoped remote unlock
	MORPCVersion29     int64 = 29 // FOUND_ROWS connection migration state
	MORPCVersion30     int64 = 30 // prepared numeric-prefix common-type casts
	MORPCVersion31     int64 = 31 // batched multi-table remote transaction unlock
	MORPCVersion32     int64 = 32 // cross-transaction logical-plan generation snapshot
	MORPCVersion33     int64 = 33 // stable complete-key distributed string shuffle hash
	MORPCVersion34     int64 = 34 // correct persisted unsigned-column metadata
	MORPCVersion35     int64 = 35 // scaled variance state with exact numeric origins
	MORPCVersion36     int64 = 36 // prepared JSON comparison execution and exact parameter types
	MORPCVersion37     int64 = 37 // independent prepared-parameter string source
	MORPCVersion38     int64 = 38 // session temporary-table connection migration
	MORPCVersion39     int64 = 39 // linearizable TN-ordered logtail read barrier
	MORPCVersion40     int64 = 40 // PAD SPACE comparison casts and set-operation equality keys
	MORPCVersion41     int64 = 41 // cycle-safe bounded current-role closure table function
	MORPCVersion42     int64 = 42 // transactional SQL-task child cleanup
	MORPCVersion43     int64 = 43 // scalar-predicate runtime-filter terminal states
	MORPCVersion44     int64 = 44 // validated MongoDB explicit-query scan payload
	MORPCVersion45     int64 = 45 // bounded Parquet whole-file fanout payload
	MORPCVersion46     int64 = 46 // subscription-aware information-schema metadata table functions
	MORPCVersion47     int64 = 47 // ordinary window hash partition pipeline algorithm
	MORPCVersion48     int64 = 48 // generation-aware CDC watermark catalog
	MORPCVersion49     int64 = 49 // vector-level grouping-set projection expansion
	MORPCVersion50     int64 = 50 // ordered ODKU evaluation and logical affected-row metadata
	MORPCVersion51     int64 = 51 // per-action ODKU validation and statement-local target arbitration
	MORPCVersion52     int64 = 52 // MySQL binary JSON subtype tags
	MORPCVersion53     int64 = 53 // ordered-stream distributed Top-N merge
	MORPCVersion54     int64 = 54 // catalog-authenticated proxy cache reuse
	MORPCVersion55     int64 = 55 // session-owned temporary DDL with transactional data
	MORPCVersion56     int64 = 56 // session-scoped AUTO_INCREMENT increment/offset and provenance
	MORPCVersion57     int64 = 57 // Arrow LOAD external-scan pipeline payload
	MORPCVersion58     int64 = 58 // binary-string function semantics and runtime-domain metadata
	MORPCVersion59     int64 = 59 // typed numeric FORMAT arguments in remote expressions
	MORPCVersion60     int64 = 60 // row-dependent expression defaults
	MORPCVersion61     int64 = 61 // index metadata provenance columns (nrow, build_ts)
	MORPCVersion62     int64 = 62 // VARCHAR OCT overload identities
	MORPCVersion63     int64 = 63 // INSERT IGNORE CHECK warning diagnostics
	MORPCVersion64     int64 = 64 // typed BIN/CONV execution contracts
	MORPCVersion65     int64 = 65 // signed INT result contract for ASCII
	MORPCVersion66     int64 = 66 // GROUP_CONCAT source-row diagnostics in aggregate partial state
	MORPCVersion67     int64 = 67 // named process timezone identity
	MORPCVersion68     int64 = 68 // complete GROUP_CONCAT cut reporting for strict writes
	MORPCVersion69     int64 = 69 // exact bounded per-partition RANK with boundary ties
	MORPCVersion70     int64 = 70 // row-dependent and unsigned CONV bases
	MORPCVersion71     int64 = 71 // checked integer arithmetic overloads
	MORPCVersion72     int64 = 72 // corrected IP function semantics and overload identities
	MORPCVersion73     int64 = 73 // widened DECIMAL SUM partial state
	MORPCVersion74     int64 = 74 // corrected numeric HEX overload identities
	MORPCVersion75     int64 = 75 // persistent data-branch database identity
	MORPCVersion76     int64 = 76 // NaN-ordered percentile and compatible HLL states
	MORPCVersion77     int64 = 77 // complete typed SQL equivalence keys in HLL states
	MORPCVersion78     int64 = 78 // length-delimited variable-length GROUP hash keys
	MORPCVersion79     int64 = 79 // canonical opaque DISTINCT argument wire keys
	MORPCVersion80     int64 = 80 // signed and widened string numeric result contracts
	MORPCVersion81     int64 = 81 // opt-in table-owned AUTO_ID_CACHE and its PRE_INSERT wire marker
	MORPCVersion82     int64 = 82 // self-completing fulltext2 json index probe (probe_tail contract)
	MORPCVersion83     int64 = 83 // bounded COALESCE character and binary result domains
	MORPCVersion84     int64 = 84 // extended discrete percentile input types
	MORPCVersion85     int64 = 85 // shared integer-parameter coercion execution identities
	MORPCVersion86     int64 = 86 // extended IP overload/result and expression metadata contracts
	MORPCVersion87     int64 = 87 // preserve grouping provenance in remote batch transport
	MORPCVersion88     int64 = 88 // canonical vector HLL_ADD_AGG hash keys
	MORPCVersion89     int64 = 89 // exact decimal literal and coercion semantics
	MORPCVersion90     int64 = 90 // geodetic distance semantics and length-unit overloads
	MORPCVersion91     int64 = 91 // canonical CHAR and JSON HLL_ADD_AGG hash keys
	MORPCVersion92     int64 = 92 // canonical scalar FLOAT HLL_ADD_AGG hash keys
	MORPCVersion93     int64 = 93 // session LAST_INSERT_ID connection migration state
	MORPCVersion94     int64 = 94 // lossless physical/logical NoFull CDC start watermark
	MORPCVersion95     int64 = 95 // prepared scalar precision execution across CNs
	MORPCVersion96     int64 = 96 // coordinator-independent vector scan object partitions
	MORPCVersion97     int64 = 97 // decimal division result semantics across CNs
	MORPCVersion98     int64 = 98 // temporal expression, interval, and WEEK contracts
	MORPCLatestVersion       = MORPCVersion98
)

// DefaultLockWaitTimeoutSeconds is shared by the frontend default and by
// distributed-pipeline compatibility code. Keeping one value is important for
// rolling upgrades: an older receiver ignores LockWaitTimeoutSet, so a newer
// sender represents an explicit clear with this positive fallback in the
// legacy LockWaitTimeout field instead of sending zero and reviving a stale
// transaction override on the receiver.
const DefaultLockWaitTimeoutSeconds int64 = 120
