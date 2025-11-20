// Copyright 2021 - 2022 Matrix Origin
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

package function

const (
	Distinct     = 0x8000000000000000
	DistinctMask = 0x7FFFFFFFFFFFFFFF
)

// All function IDs
const (
	EQUAL              = 0 // =
	NOT_EQUAL          = 1 // <>
	GREAT_THAN         = 2 // >
	GREAT_EQUAL        = 3 // >=
	LESS_THAN          = 4 // <
	LESS_EQUAL         = 5 // <=
	BETWEEN            = 6
	UNARY_PLUS         = 7  // UNARY_PLUS +
	UNARY_MINUS        = 8  // UNARY_MINUS -
	UNARY_TILDE        = 9  // UNARY_TILDE ~
	PLUS               = 10 // +
	MINUS              = 11 // -
	MULTI              = 12 // *
	DIV                = 13 // /
	INTEGER_DIV        = 14 // Div
	MOD                = 15 // %
	CONCAT             = 16 // ||
	AND                = 17
	OR                 = 18
	XOR                = 19
	NOT                = 20
	CAST               = 21
	BIT_CAST           = 22
	IS                 = 23
	ISNOT              = 24
	ISNULL             = 25
	ISNOTNULL          = 26
	ISTRUE             = 27
	ISNOTTRUE          = 28
	ISFALSE            = 29
	ISNOTFALSE         = 30
	ISEMPTY            = 31
	NOT_IN_ROWS        = 32
	OP_BIT_AND         = 33 // &
	OP_BIT_OR          = 34 // |
	OP_BIT_XOR         = 35 // ^
	OP_BIT_SHIFT_LEFT  = 36 // <<
	OP_BIT_SHIFT_RIGHT = 37 // >>

	ABS               = 38
	ACOS              = 39
	ADDDATE           = 40
	ADDTIME           = 41
	AES_DECRYPT       = 42
	AES_ENCRYPT       = 43
	ANY_VALUE         = 44
	APPROX_COUNT      = 45
	ARRAY_AGG         = 46
	ARRAY_APPEND      = 47
	ARRAY_CAT         = 48
	ARRAY_CONTAINS    = 49
	ARRAY_POSITION    = 50
	ARRAY_SIZE        = 51
	ASCII             = 52
	ASIN              = 53
	ASSERT            = 54
	ATAN              = 55
	ATAN2             = 56
	AVG               = 57
	AVG_TW_CACHE      = 58
	AVG_TW_RESULT     = 59
	BASE64_DECODE     = 60
	BASE64_ENCODE     = 61
	BIT_AND           = 62
	BIT_LENGTH        = 63
	BIT_NOT           = 64
	BIT_OR            = 65
	BIT_XOR           = 66
	BITAGG_AND        = 67
	BITAGG_OR         = 68
	BOOLAGG_AND       = 69
	BOOLAGG_OR        = 70
	CASE              = 71
	CEIL              = 72
	CHR               = 73
	COALESCE          = 74
	FIELD             = 75
	CONCAT_WS         = 76
	CONTAINS          = 77
	CORR              = 78
	COS               = 79
	COT               = 80
	CRC32             = 81
	COUNT             = 82
	COUNT_IF          = 83
	COVAR_POP         = 84
	COVAR_SAMPLE      = 85
	CONVERT_TZ        = 86
	CUME_DIST         = 87
	CURRENT_DATE      = 88
	CURRENT_TIMESTAMP = 89
	DATE_FROM_PARTS   = 90
	DATE_PART         = 91
	DATEADD           = 92
	DATEDIFF          = 93
	TIMEDIFF          = 94
	TIMESTAMPDIFF     = 95
	DENSE_RANK        = 96
	MO_WIN_DIVISOR    = 97
	EMPTY             = 98
	ENDSWITH          = 99
	EXP               = 100
	FINDINSET         = 101
	FIRST_VALUE       = 102
	FLOOR             = 103
	GREATEST          = 104
	GROUPING          = 105
	HASH              = 106
	HASH_AGG          = 107
	HEX_DECODE        = 108
	HEX_ENCODE        = 109
	HEX               = 110
	UNHEX             = 111
	MD5               = 112
	IFF               = 113
	IFNULL            = 114
	ILIKE             = 115
	ILIKE_ALL         = 116
	ILIKE_ANY         = 117
	IN                = 118
	LAG               = 119
	LAST_VALUE        = 120
	LEAD              = 121
	LEAST             = 122
	LEFT              = 123
	LENGTH            = 124
	LENGTH_UTF8       = 125
	LIKE              = 126
	LIKE_ALL          = 127
	LIKE_ANY          = 128
	LN                = 129
	NOT_IN            = 130
	LOG               = 131
	LOG2              = 132
	LOG10             = 133
	LOWER             = 134
	LPAD              = 135
	LTRIM             = 136
	MAX               = 137
	MEDIAN            = 138
	MIN               = 139
	MODE              = 140
	MONTH             = 141
	NORMAL            = 142
	NTH_VALUE         = 143
	NTILE             = 144
	NULLIF            = 145
	PERCENT_RANK      = 146
	PI                = 147
	POSITION          = 148
	POW               = 149
	PREFIX_EQ         = 150
	PREFIX_IN         = 151
	PREFIX_BETWEEN    = 152
	RADIAN            = 153
	RANDOM            = 154
	RANK              = 155
	REGEXP            = 156
	REGEXP_INSTR      = 157
	REGEXP_LIKE       = 158
	REGEXP_REPLACE    = 159
	REGEXP_SUBSTR     = 160
	REG_MATCH         = 161
	NOT_REG_MATCH     = 162
	REPEAT            = 163
	REPLACE           = 164
	REVERSE           = 165
	RIGHT             = 166
	ROUND             = 167
	ROW_NUMBER        = 168
	RPAD              = 169
	RTRIM             = 170
	SIGN              = 171
	SIN               = 172
	SINH              = 173
	SPACE             = 174
	SPLIT             = 175
	SPLIT_PART        = 176
	SQRT              = 177
	STARCOUNT         = 178
	STARTSWITH        = 179
	STDDEV_POP        = 180
	STDDEV_SAMPLE     = 181
	SUBSTR            = 182
	SUM               = 183
	SYSDATE           = 184
	GROUP_CONCAT      = 185
	TAN               = 186
	TO_DATE           = 187
	STR_TO_DATE       = 188
	TO_INTERVAL       = 189
	TRANSLATE         = 190
	TRIM              = 191
	UNIFORM           = 192
	SHA1              = 193
	SHA2              = 194
	UTC_TIMESTAMP     = 195
	UNIX_TIMESTAMP    = 196
	FROM_UNIXTIME     = 197
	UPPER             = 198
	VAR_POP           = 199
	VAR_SAMPLE        = 200

	// Date and Time functions
	LAST_DAY = 201
	MAKEDATE = 202

	DATE            = 203
	TIME            = 204
	DAY             = 205
	DAYOFYEAR       = 206
	DAYOFWEEK       = 207
	INTERVAL        = 208
	EXTRACT         = 209
	OCT             = 210
	SUBSTRING       = 211
	ENCODE          = 212
	DECODE          = 213
	TO_BASE64       = 214
	FROM_BASE64     = 215
	SUBSTRING_INDEX = 216
	WEEK            = 217
	WEEKDAY         = 218
	YEAR            = 219
	HOUR            = 220
	MINUTE          = 221
	SECOND          = 222
	TO_DAYS         = 223
	TO_SECONDS      = 224

	DATE_ADD              = 225
	DATE_SUB              = 226
	APPROX_COUNT_DISTINCT = 227

	LOAD_FILE = 228
	SAVE_FILE = 229

	//information functions
	//Reference to : https://dev.mysql.com/doc/refman/8.0/en/information-functions.html
	DATABASE             = 230
	USER                 = 231
	CONNECTION_ID        = 232
	CHARSET              = 233
	CONVERT              = 234
	CURRENT_ROLE         = 235
	FOUND_ROWS           = 236
	ICULIBVERSION        = 237
	LAST_INSERT_ID       = 238
	LAST_QUERY_ID        = 239
	LAST_UUID            = 240
	ROLES_GRAPHML        = 241
	ROW_COUNT            = 242
	VERSION              = 243
	COLLATION            = 244
	CURRENT_ACCOUNT_ID   = 245
	CURRENT_ACCOUNT_NAME = 246
	CURRENT_ROLE_ID      = 247
	CURRENT_ROLE_NAME    = 248
	CURRENT_USER_ID      = 249
	CURRENT_USER_NAME    = 250

	TIMESTAMP            = 251
	DATE_FORMAT          = 252
	JSON_EXTRACT         = 253
	JSON_EXTRACT_STRING  = 254
	JSON_EXTRACT_FLOAT64 = 255
	JSON_QUOTE           = 256
	JSON_UNQUOTE         = 257
	JSON_ROW             = 258

	JQ       = 259
	TRY_JQ   = 260
	WASM     = 261
	TRY_WASM = 262
	FORMAT   = 263
	SLEEP    = 264
	INSTR    = 265
	LOCATE   = 266

	UUID           = 267
	SERIAL         = 268
	SERIAL_FULL    = 269
	SERIAL_EXTRACT = 270
	BIN            = 271

	ENABLE_FAULT_INJECTION  = 272
	DISABLE_FAULT_INJECTION = 273
	ADD_FAULT_POINT         = 274 // Add a fault point
	REMOVE_FAULT_POINT      = 275 // Remove
	TRIGGER_FAULT_POINT     = 276 // Trigger.
	MO_WIN_TRUNCATE         = 277

	MO_MEMORY_USAGE                = 278 // Dump memory usage
	MO_ENABLE_MEMORY_USAGE_DETAIL  = 279
	MO_DISABLE_MEMORY_USAGE_DETAIL = 280

	// MO_CTL is used to check some internal status, and issue some ctl commands to the service.
	// see builtin.ctl.ctl.go to get detail.
	MO_CTL = 281

	MO_SHOW_VISIBLE_BIN      = 282 // parse type/onUpdate/default []byte to visible string
	MO_SHOW_VISIBLE_BIN_ENUM = 283 //  parse type/onUpdate/default []byte to visible string for enum
	MO_SHOW_COL_UNIQUE       = 284 // show column whether unique key

	MO_TABLE_ROWS    = 285 // table rows
	MO_TABLE_SIZE    = 286 // table size
	MO_TABLE_COL_MAX = 287 // table column max value
	MO_TABLE_COL_MIN = 288 // table column min value

	MO_LOG_DATE    = 289 // parse date from string, like __mo_filepath
	PURGE_LOG      = 290 // purge mo internal log, like rawlog, statement_info, metric
	MO_ADMIN_NAME  = 291 // get mo admin name of account
	MO_CU          = 292
	MO_CU_V1       = 293
	MO_EXPLAIN_PHY = 294

	GIT_VERSION   = 295
	BUILD_VERSION = 296

	// be used: insert into t1 values(1,1) on duplicate key update a=values(a)+a+1
	VALUES                        = 297
	BINARY                        = 298
	INTERNAL_CHAR_LENGTH          = 299
	INTERNAL_CHAR_SIZE            = 300
	INTERNAL_NUMERIC_PRECISION    = 301
	INTERNAL_NUMERIC_SCALE        = 302
	INTERNAL_DATETIME_SCALE       = 303
	INTERNAL_COLUMN_CHARACTER_SET = 304
	INTERNAL_AUTO_INCREMENT       = 305

	// be used: enum
	CAST_INDEX_TO_VALUE       = 306
	CAST_VALUE_TO_INDEX       = 307
	CAST_INDEX_VALUE_TO_INDEX = 308

	// be used: show snapshots
	CAST_NANO_TO_TIMESTAMP = 309
	// be used: show pitr
	CAST_RANGE_VALUE_UNIT = 310

	//Sequence function
	NEXTVAL = 311
	SETVAL  = 312
	CURRVAL = 313
	LASTVAL = 314

	// Array Function
	SUMMATION         = 315
	L1_NORM           = 316 // L1_NORMALIZATION
	L2_NORM           = 317 // L2 NORMALIZATION
	INNER_PRODUCT     = 318
	COSINE_SIMILARITY = 319
	VECTOR_DIMS       = 320 //VECTOR DIMENSIONS
	NORMALIZE_L2      = 321 //NORMALIZE L2
	L2_DISTANCE       = 322 //L2_DISTANCE
	L2_DISTANCE_SQ    = 323 //L2_DISTANCE_SQ
	COSINE_DISTANCE   = 324 //COSINE_DISTANCE
	CLUSTER_CENTERS   = 325 // CLUSTER_CENTERS
	SUB_VECTOR        = 326 // SUB_VECTOR

	PYTHON_UDF = 327

	// observation function
	MO_CPU      = 328
	MO_MEMORY   = 329
	MO_CPU_DUMP = 330

	// bitmap function
	BITMAP_BIT_POSITION  = 331
	BITMAP_BUCKET_NUMBER = 332
	BITMAP_COUNT         = 333
	BITMAP_CONSTRUCT_AGG = 334
	BITMAP_OR_AGG        = 335

	// fulltext function
	FULLTEXT_MATCH       = 336
	FULLTEXT_MATCH_SCORE = 337

	JSON_SET     = 338
	JSON_INSERT  = 339
	JSON_REPLACE = 340

	// fault inject function
	FAULT_INJECT = 341

	L2_DISTANCE_XC    = 342
	L2_DISTANCE_SQ_XC = 343

	TS_TO_TIME = 344
	STRCMP     = 345

	STARLARK     = 346
	TRY_STARLARK = 347

	// FUNCTION_END_NUMBER is not a function, just a flag to record the max number of function.
	// TODO: every one should put the new function id in front of this one if you want to make a new function.
	FUNCTION_END_NUMBER = 348
)

// functionIdRegister is what function we have registered already.
var functionIdRegister = map[string]int32{
	// operators
	"=":              EQUAL,
	">":              GREAT_THAN,
	">=":             GREAT_EQUAL,
	"<":              LESS_THAN,
	"<=":             LESS_EQUAL,
	"<>":             NOT_EQUAL,
	"!=":             NOT_EQUAL,
	"not":            NOT,
	"and":            AND,
	"or":             OR,
	"xor":            XOR,
	"like":           LIKE,
	"between":        BETWEEN,
	"in":             IN,
	"not_in":         NOT_IN,
	"+":              PLUS,
	"-":              MINUS,
	"*":              MULTI,
	"/":              DIV,
	"div":            INTEGER_DIV,
	"%":              MOD,
	"mod":            MOD,
	"unary_plus":     UNARY_PLUS,
	"unary_minus":    UNARY_MINUS,
	"unary_tilde":    UNARY_TILDE,
	"unary_mark":     NOT,
	"case":           CASE,
	"coalesce":       COALESCE,
	"cast":           CAST,
	"bit_cast":       BIT_CAST,
	"is":             IS,
	"is_not":         ISNOT,
	"isnot":          ISNOT,
	"is_null":        ISNULL,
	"isnull":         ISNULL,
	"ifnull":         ISNULL,
	"ilike":          ILIKE,
	"is_not_null":    ISNOTNULL,
	"isnotnull":      ISNOTNULL,
	"isunknown":      ISNULL,
	"isnotunknown":   ISNOTNULL,
	"istrue":         ISTRUE,
	"isnottrue":      ISNOTTRUE,
	"isfalse":        ISFALSE,
	"isnotfalse":     ISNOTFALSE,
	"&":              OP_BIT_AND,
	"|":              OP_BIT_OR,
	"^":              OP_BIT_XOR,
	"<<":             OP_BIT_SHIFT_LEFT,
	">>":             OP_BIT_SHIFT_RIGHT,
	"decode":         DECODE,
	"prefix_eq":      PREFIX_EQ,
	"prefix_in":      PREFIX_IN,
	"prefix_between": PREFIX_BETWEEN,
	// aggregate
	"max":                   MAX,
	"min":                   MIN,
	"sum":                   SUM,
	"group_concat":          GROUP_CONCAT,
	"grouping":              GROUPING,
	"avg":                   AVG,
	"avg_tw_cache":          AVG_TW_CACHE,
	"avg_tw_result":         AVG_TW_RESULT,
	"count":                 COUNT,
	"starcount":             STARCOUNT,
	"bit_or":                BIT_OR,
	"bit_and":               BIT_AND,
	"bit_xor":               BIT_XOR,
	"cluster_centers":       CLUSTER_CENTERS,
	"subvector":             SUB_VECTOR,
	"std":                   STDDEV_POP,
	"stddev_pop":            STDDEV_POP,
	"variance":              VAR_POP,
	"var_pop":               VAR_POP,
	"approx_count":          APPROX_COUNT,
	"approx_count_distinct": APPROX_COUNT_DISTINCT,
	"any_value":             ANY_VALUE,
	"median":                MEDIAN,
	// count window
	"rank": RANK,
	// builtin
	// whoever edit this, please follow the lexical order, or come up with a better ordering method
	// binary functions
	"endswith":    ENDSWITH,
	"findinset":   FINDINSET,
	"find_in_set": FINDINSET,
	"power":       POW,
	"startswith":  STARTSWITH,
	"to_date":     STR_TO_DATE,
	"str_to_date": STR_TO_DATE,
	"ts_to_time":  TS_TO_TIME,
	"date_format": DATE_FORMAT,
	// whoever edit this, please follow the lexical order, or come up with a better ordering method
	// variadic functions
	"ceil":              CEIL,
	"ceiling":           CEIL,
	"concat_ws":         CONCAT_WS,
	"concat":            CONCAT,
	"current_timestamp": CURRENT_TIMESTAMP,
	"now":               CURRENT_TIMESTAMP,
	"sysdate":           SYSDATE,
	"floor":             FLOOR,
	"lpad":              LPAD,
	"pi":                PI,
	"round":             ROUND,
	"rpad":              RPAD,
	"strcmp":            STRCMP,
	"substr":            SUBSTRING,
	"substring":         SUBSTRING,
	"mid":               SUBSTRING,
	"encode":            ENCODE,
	"utc_timestamp":     UTC_TIMESTAMP,
	"unix_timestamp":    UNIX_TIMESTAMP,
	"from_unixtime":     FROM_UNIXTIME,
	"left":              LEFT,
	// unary functions
	// whoever edit this, please follow the lexical order, or come up with a better ordering method
	"abs":                            ABS,
	"acos":                           ACOS,
	"assert":                         ASSERT,
	"bit_length":                     BIT_LENGTH,
	"date":                           DATE,
	"time":                           TIME,
	"hour":                           HOUR,
	"minute":                         MINUTE,
	"second":                         SECOND,
	"sqrt":                           SQRT,
	"to_seconds":                     TO_SECONDS,
	"day":                            DAY,
	"to_days":                        TO_DAYS,
	"dayofyear":                      DAYOFYEAR,
	"dayofweek":                      DAYOFWEEK,
	"exp":                            EXP,
	"empty":                          EMPTY,
	"length":                         LENGTH,
	"lengthutf8":                     LENGTH_UTF8,
	"char_length":                    LENGTH_UTF8,
	"ln":                             LN,
	"log":                            LOG,
	"log2":                           LOG2,
	"log10":                          LOG10,
	"ltrim":                          LTRIM,
	"month":                          MONTH,
	"not_in_rows":                    NOT_IN_ROWS,
	"oct":                            OCT,
	"rand":                           RANDOM,
	"reverse":                        REVERSE,
	"rtrim":                          RTRIM,
	"sin":                            SIN,
	"sinh":                           SINH,
	"space":                          SPACE,
	"tan":                            TAN,
	"week":                           WEEK,
	"weekday":                        WEEKDAY,
	"year":                           YEAR,
	"extract":                        EXTRACT,
	"if":                             IFF,
	"iff":                            IFF,
	"isempty":                        ISEMPTY,
	"date_add":                       DATE_ADD,
	"date_sub":                       DATE_SUB,
	"atan":                           ATAN,
	"cos":                            COS,
	"cot":                            COT,
	"crc32":                          CRC32,
	"timestamp":                      TIMESTAMP,
	"database":                       DATABASE,
	"schema":                         DATABASE,
	"user":                           USER,
	"system_user":                    USER,
	"session_user":                   USER,
	"current_user":                   USER,
	"connection_id":                  CONNECTION_ID,
	"charset":                        CHARSET,
	"convert":                        CONVERT,
	"convert_tz":                     CONVERT_TZ,
	"current_account_id":             CURRENT_ACCOUNT_ID,
	"current_account_name":           CURRENT_ACCOUNT_NAME,
	"current_role":                   CURRENT_ROLE,
	"current_role_id":                CURRENT_ROLE_ID,
	"current_role_name":              CURRENT_ROLE_NAME,
	"current_user_id":                CURRENT_USER_ID,
	"current_user_name":              CURRENT_USER_NAME,
	"found_rows":                     FOUND_ROWS,
	"icu_version":                    ICULIBVERSION,
	"last_insert_id":                 LAST_INSERT_ID,
	"last_query_id":                  LAST_QUERY_ID,
	"last_uuid":                      LAST_QUERY_ID,
	"roles_graphml":                  ROLES_GRAPHML,
	"row_count":                      ROW_COUNT,
	"row_number":                     ROW_NUMBER,
	"version":                        VERSION,
	"collation":                      COLLATION,
	"json_extract":                   JSON_EXTRACT,
	"json_extract_string":            JSON_EXTRACT_STRING,
	"json_extract_float64":           JSON_EXTRACT_FLOAT64,
	"json_quote":                     JSON_QUOTE,
	"json_unquote":                   JSON_UNQUOTE,
	"json_row":                       JSON_ROW,
	"json_set":                       JSON_SET,
	"json_insert":                    JSON_INSERT,
	"json_replace":                   JSON_REPLACE,
	"jq":                             JQ,
	"try_jq":                         TRY_JQ,
	"moplugin":                       WASM,
	"try_moplugin":                   TRY_WASM,
	"enable_fault_injection":         ENABLE_FAULT_INJECTION,
	"disable_fault_injection":        DISABLE_FAULT_INJECTION,
	"dense_rank":                     DENSE_RANK,
	"mo_win_divisor":                 MO_WIN_DIVISOR,
	"add_fault_point":                ADD_FAULT_POINT,
	"remove_fault_point":             REMOVE_FAULT_POINT,
	"trigger_fault_point":            TRIGGER_FAULT_POINT,
	"mo_win_truncate":                MO_WIN_TRUNCATE,
	"uuid":                           UUID,
	"load_file":                      LOAD_FILE,
	"save_file":                      SAVE_FILE,
	"hex":                            HEX,
	"unhex":                          UNHEX,
	"md5":                            MD5,
	"to_base64":                      TO_BASE64,
	"from_base64":                    FROM_BASE64,
	"serial":                         SERIAL,
	"serial_full":                    SERIAL_FULL,
	"serial_extract":                 SERIAL_EXTRACT,
	"hash_value":                     HASH,
	"bin":                            BIN,
	"datediff":                       DATEDIFF,
	"timestampdiff":                  TIMESTAMPDIFF,
	"timediff":                       TIMEDIFF,
	"last_day":                       LAST_DAY,
	"makedate":                       MAKEDATE,
	"reg_match":                      REG_MATCH,
	"not_reg_match":                  NOT_REG_MATCH,
	"regexp_instr":                   REGEXP_INSTR,
	"regexp_like":                    REGEXP_LIKE,
	"regexp_replace":                 REGEXP_REPLACE,
	"regexp_substr":                  REGEXP_SUBSTR,
	"repeat":                         REPEAT,
	"mo_memory_usage":                MO_MEMORY_USAGE,
	"mo_enable_memory_usage_detail":  MO_ENABLE_MEMORY_USAGE_DETAIL,
	"mo_disable_memory_usage_detail": MO_DISABLE_MEMORY_USAGE_DETAIL,
	"mo_ctl":                         MO_CTL,
	"mo_show_visible_bin":            MO_SHOW_VISIBLE_BIN,
	"mo_show_visible_bin_enum":       MO_SHOW_VISIBLE_BIN_ENUM,
	"mo_show_col_unique":             MO_SHOW_COL_UNIQUE,
	"substring_index":                SUBSTRING_INDEX,
	"field":                          FIELD,
	"format":                         FORMAT,
	"sleep":                          SLEEP,
	"split_part":                     SPLIT_PART,
	"instr":                          INSTR,
	"locate":                         LOCATE,
	"curdate":                        CURRENT_DATE,
	"current_date":                   CURRENT_DATE,
	"ascii":                          ASCII,
	"replace":                        REPLACE,
	"mo_table_rows":                  MO_TABLE_ROWS,
	"mo_table_size":                  MO_TABLE_SIZE,
	"mo_table_col_max":               MO_TABLE_COL_MAX,
	"mo_table_col_min":               MO_TABLE_COL_MIN,
	"trim":                           TRIM,
	"sha2":                           SHA2,
	"mo_log_date":                    MO_LOG_DATE,
	"purge_log":                      PURGE_LOG,
	"mo_admin_name":                  MO_ADMIN_NAME,
	"mo_cu":                          MO_CU,
	"mo_cu_v1":                       MO_CU_V1,
	"mo_explain_phy":                 MO_EXPLAIN_PHY,
	"git_version":                    GIT_VERSION,
	"build_version":                  BUILD_VERSION,
	"values":                         VALUES,
	"binary":                         BINARY,
	"internal_char_length":           INTERNAL_CHAR_LENGTH,
	"internal_char_size":             INTERNAL_CHAR_SIZE,
	"internal_numeric_precision":     INTERNAL_NUMERIC_PRECISION,
	"internal_numeric_scale":         INTERNAL_NUMERIC_SCALE,
	"internal_datetime_scale":        INTERNAL_DATETIME_SCALE,
	"internal_column_character_set":  INTERNAL_COLUMN_CHARACTER_SET,
	"internal_auto_increment":        INTERNAL_AUTO_INCREMENT,
	"nextval":                        NEXTVAL,
	"setval":                         SETVAL,
	"currval":                        CURRVAL,
	"lastval":                        LASTVAL,
	"cast_index_to_value":            CAST_INDEX_TO_VALUE,
	"cast_value_to_index":            CAST_VALUE_TO_INDEX,
	"cast_index_value_to_index":      CAST_INDEX_VALUE_TO_INDEX,
	"cast_nano_to_timestamp":         CAST_NANO_TO_TIMESTAMP,
	"cast_range_value_unit":          CAST_RANGE_VALUE_UNIT,
	"to_upper":                       UPPER,
	"upper":                          UPPER,
	"ucase":                          UPPER,
	"to_lower":                       LOWER,
	"lower":                          LOWER,
	"lcase":                          LOWER,
	"sha1":                           SHA1,
	"sha":                            SHA1,

	"summation":         SUMMATION,
	"l1_norm":           L1_NORM,
	"l2_norm":           L2_NORM,
	"inner_product":     INNER_PRODUCT,
	"cosine_similarity": COSINE_SIMILARITY,
	"vector_dims":       VECTOR_DIMS,
	"normalize_l2":      NORMALIZE_L2,
	"l2_distance":       L2_DISTANCE,
	"l2_distance_xc":    L2_DISTANCE_XC,
	"l2_distance_sq":    L2_DISTANCE_SQ,
	"l2_distance_sq_xc": L2_DISTANCE_SQ_XC,
	"cosine_distance":   COSINE_DISTANCE,

	"python_user_defined_function": PYTHON_UDF,

	"mo_cpu":      MO_CPU,
	"mo_memory":   MO_MEMORY,
	"mo_cpu_dump": MO_CPU_DUMP,
	// bitmap function
	"bitmap_bit_position":  BITMAP_BIT_POSITION,
	"bitmap_bucket_number": BITMAP_BUCKET_NUMBER,
	"bitmap_count":         BITMAP_COUNT,
	"bitmap_construct_agg": BITMAP_CONSTRUCT_AGG,
	"bitmap_or_agg":        BITMAP_OR_AGG,

	// match function
	"fulltext_match":       FULLTEXT_MATCH,
	"fulltext_match_score": FULLTEXT_MATCH_SCORE,

	// starlark function
	"starlark":     STARLARK,
	"try_starlark": TRY_STARLARK,

	// fault inject function
	"fault_inject": FAULT_INJECT,
}
