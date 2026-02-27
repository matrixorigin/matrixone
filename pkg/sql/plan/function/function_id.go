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
	INTERVAL        = 207
	EXTRACT         = 208
	OCT             = 209
	SUBSTRING       = 210
	ENCODE          = 211
	DECODE          = 212
	TO_BASE64       = 213
	FROM_BASE64     = 214
	SUBSTRING_INDEX = 215
	WEEK            = 216
	WEEKDAY         = 217
	YEAR            = 218
	HOUR            = 219
	MINUTE          = 220
	SECOND          = 221
	TO_DAYS         = 222
	TO_SECONDS      = 223

	DATE_ADD              = 224
	DATE_SUB              = 225
	APPROX_COUNT_DISTINCT = 226

	LOAD_FILE = 227
	SAVE_FILE = 228

	//information functions
	//Reference to : https://dev.mysql.com/doc/refman/8.0/en/information-functions.html
	DATABASE             = 229
	USER                 = 230
	CONNECTION_ID        = 231
	CHARSET              = 232
	CONVERT              = 233
	CURRENT_ROLE         = 234
	FOUND_ROWS           = 235
	ICULIBVERSION        = 236
	LAST_INSERT_ID       = 237
	LAST_QUERY_ID        = 238
	LAST_UUID            = 239
	ROLES_GRAPHML        = 240
	ROW_COUNT            = 241
	VERSION              = 242
	COLLATION            = 243
	CURRENT_ACCOUNT_ID   = 244
	CURRENT_ACCOUNT_NAME = 245
	CURRENT_ROLE_ID      = 246
	CURRENT_ROLE_NAME    = 247
	CURRENT_USER_ID      = 248
	CURRENT_USER_NAME    = 249

	TIMESTAMP            = 250
	DATE_FORMAT          = 251
	JSON_EXTRACT         = 252
	JSON_EXTRACT_STRING  = 253
	JSON_EXTRACT_FLOAT64 = 254
	JSON_QUOTE           = 255
	JSON_UNQUOTE         = 256
	JSON_ROW             = 257

	JQ       = 258
	TRY_JQ   = 259
	WASM     = 260
	TRY_WASM = 261
	FORMAT   = 262
	SLEEP    = 263
	INSTR    = 264
	LOCATE   = 265

	UUID           = 266
	SERIAL         = 267
	SERIAL_FULL    = 268
	SERIAL_EXTRACT = 269
	BIN            = 270

	ENABLE_FAULT_INJECTION  = 271
	DISABLE_FAULT_INJECTION = 272
	ADD_FAULT_POINT         = 273 // Add a fault point
	REMOVE_FAULT_POINT      = 274 // Remove
	TRIGGER_FAULT_POINT     = 275 // Trigger.
	MO_WIN_TRUNCATE         = 276

	MO_MEMORY_USAGE                = 277 // Dump memory usage
	MO_ENABLE_MEMORY_USAGE_DETAIL  = 278
	MO_DISABLE_MEMORY_USAGE_DETAIL = 279

	// MO_CTL is used to check some internal status, and issue some ctl commands to the service.
	// see builtin.ctl.ctl.go to get detail.
	MO_CTL = 280

	MO_SHOW_VISIBLE_BIN      = 281 // parse type/onUpdate/default []byte to visible string
	MO_SHOW_VISIBLE_BIN_ENUM = 282 //  parse type/onUpdate/default []byte to visible string for enum
	MO_SHOW_COL_UNIQUE       = 283 // show column whether unique key

	MO_TABLE_ROWS    = 284 // table rows
	MO_TABLE_SIZE    = 285 // table size
	MO_TABLE_COL_MAX = 286 // table column max value
	MO_TABLE_COL_MIN = 287 // table column min value

	MO_LOG_DATE    = 288 // parse date from string, like __mo_filepath
	PURGE_LOG      = 289 // purge mo internal log, like rawlog, statement_info, metric
	MO_ADMIN_NAME  = 290 // get mo admin name of account
	MO_CU          = 291
	MO_CU_V1       = 292
	MO_EXPLAIN_PHY = 293

	GIT_VERSION   = 294
	BUILD_VERSION = 295

	// be used: insert into t1 values(1,1) on duplicate key update a=values(a)+a+1
	VALUES                        = 296
	BINARY                        = 297
	INTERNAL_CHAR_LENGTH          = 298
	INTERNAL_CHAR_SIZE            = 299
	INTERNAL_NUMERIC_PRECISION    = 300
	INTERNAL_NUMERIC_SCALE        = 301
	INTERNAL_DATETIME_SCALE       = 302
	INTERNAL_COLUMN_CHARACTER_SET = 303
	INTERNAL_AUTO_INCREMENT       = 304

	// be used: enum
	CAST_INDEX_TO_VALUE       = 305
	CAST_VALUE_TO_INDEX       = 306
	CAST_INDEX_VALUE_TO_INDEX = 307

	// be used: show snapshots
	CAST_NANO_TO_TIMESTAMP = 308
	// be used: show pitr
	CAST_RANGE_VALUE_UNIT = 309

	//Sequence function
	NEXTVAL = 310
	SETVAL  = 311
	CURRVAL = 312
	LASTVAL = 313

	// Array Function
	SUMMATION         = 314
	L1_NORM           = 315 // L1_NORMALIZATION
	L2_NORM           = 316 // L2 NORMALIZATION
	INNER_PRODUCT     = 317
	COSINE_SIMILARITY = 318
	VECTOR_DIMS       = 319 //VECTOR DIMENSIONS
	NORMALIZE_L2      = 320 //NORMALIZE L2
	L2_DISTANCE       = 321 //L2_DISTANCE
	L2_DISTANCE_SQ    = 322 //L2_DISTANCE_SQ
	COSINE_DISTANCE   = 323 //COSINE_DISTANCE
	CLUSTER_CENTERS   = 324 // CLUSTER_CENTERS
	SUB_VECTOR        = 325 // SUB_VECTOR

	PYTHON_UDF = 326

	// observation function
	MO_CPU      = 327
	MO_MEMORY   = 328
	MO_CPU_DUMP = 329

	// bitmap function
	BITMAP_BIT_POSITION  = 330
	BITMAP_BUCKET_NUMBER = 331
	BITMAP_COUNT         = 332
	BITMAP_CONSTRUCT_AGG = 333
	BITMAP_OR_AGG        = 334

	// fulltext function
	FULLTEXT_MATCH       = 335
	FULLTEXT_MATCH_SCORE = 336

	JSON_SET     = 337
	JSON_INSERT  = 338
	JSON_REPLACE = 339

	// fault inject function
	FAULT_INJECT = 340

	L2_DISTANCE_XC    = 341
	L2_DISTANCE_SQ_XC = 342

	TS_TO_TIME = 343
	STRCMP     = 344

	STARLARK     = 345
	TRY_STARLARK = 346

	LLM_CHAT      = 347
	LLM_EMBEDDING = 348

	// hash partition function
	HASH_PARTITION = 349

	// mo_tuple_expr function
	MO_TUPLE_EXPR = 350

	// function `current_time`, `curtime`
	CURRENT_TIME = 351

	// function `truncate`
	TRUNCATE = 352

	// function `char`
	CHAR = 353

	// function `insert`
	INSERT = 354

	// function `ord`
	ORD = 355

	// function `quote`
	QUOTE = 356

	// function `soundex`
	SOUNDEX = 357

	// function `degrees`
	DEGREES = 358

	// function `radians`
	RADIANS = 359

	// function `dayofweek`
	DAYOFWEEK = 360

	// function `microsecond`
	MICROSECOND = 361

	// function `quarter`
	QUARTER = 362

	// function `time_format`
	TIME_FORMAT = 363

	// function `timestampadd`
	TIMESTAMPADD = 364

	// function `json_array`
	JSON_ARRAY = 365

	// function `json_object`
	JSON_OBJECT = 366

	// function `conv`
	CONV = 367

	// function `dayname`
	DAYNAME = 368

	// function `dayofmonth`
	DAYOFMONTH = 369

	// function `from_days`
	FROM_DAYS = 370

	// function `get_format`
	GET_FORMAT = 371

	// function `localtime`
	LOCALTIME = 372

	// function `maketime`
	MAKETIME = 373

	// function `monthname`
	MONTHNAME = 374

	// function `period_add`
	PERIOD_ADD = 375

	// function `period_diff`
	PERIOD_DIFF = 376

	// function `sec_to_time`
	SEC_TO_TIME = 377

	// function `subtime`
	SUBTIME = 378

	// function `time_to_sec`
	TIME_TO_SEC = 379

	// function `utc_date`
	UTC_DATE = 380

	// function `utc_time`
	UTC_TIME = 381

	// function `weekofyear`
	WEEKOFYEAR = 382

	// function `yearweek`
	YEARWEEK = 383

	// function `elt`
	ELT = 384

	// function `export_set`
	EXPORT_SET = 385

	// function `make_set`
	MAKE_SET = 386

	// function `compress`
	COMPRESS = 387

	// function `uncompress`
	UNCOMPRESS = 388

	// function `uncompressed_length`
	UNCOMPRESSED_LENGTH = 389

	// function `random_bytes`
	RANDOM_BYTES = 390

	// function `validate_password_strength`
	VALIDATE_PASSWORD_STRENGTH = 391

	// function `inet6_aton`
	INET6_ATON = 392

	// function `inet6_ntoa`
	INET6_NTOA = 393

	// function `inet_aton`
	INET_ATON = 394

	// function `inet_ntoa`
	INET_NTOA = 395

	// function `is_ipv4`
	IS_IPV4 = 396

	// function `is_ipv6`
	IS_IPV6 = 397

	// function `is_ipv4_compat`
	IS_IPV4_COMPAT = 398

	// function `is_ipv4_mapped`
	IS_IPV4_MAPPED = 399

	// function `json_arrayagg`
	JSON_ARRAYAGG = 400

	// function `json_objectagg`
	JSON_OBJECTAGG = 401

	// function `mo_feature_registry_upsert`
	MO_FEATURE_REGISTRY_UPSERT = 402

	// function `mo_feature_limit_upsert`
	MO_FEATURE_LIMIT_UPSERT = 403

	IN_RANGE        = 404
	PREFIX_IN_RANGE = 405

	// FUNCTION_END_NUMBER is not a function, just a flag to record the max number of function.
	// TODO: every one should put the new function id in front of this one if you want to make a new function.
	FUNCTION_END_NUMBER = 406
)

// functionIdRegister is what function we have registered already.
var functionIdRegister = map[string]int32{
	// operators
	"=":            EQUAL,
	">":            GREAT_THAN,
	">=":           GREAT_EQUAL,
	"<":            LESS_THAN,
	"<=":           LESS_EQUAL,
	"<>":           NOT_EQUAL,
	"!=":           NOT_EQUAL,
	"not":          NOT,
	"and":          AND,
	"or":           OR,
	"xor":          XOR,
	"like":         LIKE,
	"between":      BETWEEN,
	"in":           IN,
	"not_in":       NOT_IN,
	"in_range":     IN_RANGE,
	"+":            PLUS,
	"-":            MINUS,
	"*":            MULTI,
	"/":            DIV,
	"div":          INTEGER_DIV,
	"%":            MOD,
	"mod":          MOD,
	"unary_plus":   UNARY_PLUS,
	"unary_minus":  UNARY_MINUS,
	"unary_tilde":  UNARY_TILDE,
	"unary_mark":   NOT,
	"case":         CASE,
	"coalesce":     COALESCE,
	"cast":         CAST,
	"bit_cast":     BIT_CAST,
	"is":           IS,
	"is_not":       ISNOT,
	"isnot":        ISNOT,
	"is_null":      ISNULL,
	"isnull":       ISNULL,
	"ifnull":       ISNULL,
	"ilike":        ILIKE,
	"is_not_null":  ISNOTNULL,
	"isnotnull":    ISNOTNULL,
	"isunknown":    ISNULL,
	"isnotunknown": ISNOTNULL,
	"istrue":       ISTRUE,
	"isnottrue":    ISNOTTRUE,
	"isfalse":      ISFALSE,
	"isnotfalse":   ISNOTFALSE,
	"&":            OP_BIT_AND,
	"|":            OP_BIT_OR,
	"^":            OP_BIT_XOR,
	"<<":           OP_BIT_SHIFT_LEFT,
	">>":           OP_BIT_SHIFT_RIGHT,
	"decode":       DECODE,

	"prefix_eq":       PREFIX_EQ,
	"prefix_in":       PREFIX_IN,
	"prefix_between":  PREFIX_BETWEEN,
	"prefix_in_range": PREFIX_IN_RANGE,
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
	"stddev":                STDDEV_POP,
	"stddev_pop":            STDDEV_POP,
	"stddev_samp":           STDDEV_SAMPLE,
	"variance":              VAR_POP,
	"var_pop":               VAR_POP,
	"var_samp":              VAR_SAMPLE,
	"approx_count":          APPROX_COUNT,
	"approx_count_distinct": APPROX_COUNT_DISTINCT,
	"any_value":             ANY_VALUE,
	"median":                MEDIAN,
	// count window
	"rank":       RANK,
	"row_number": ROW_NUMBER,
	"dense_rank": DENSE_RANK,
	"ntile":      NTILE,
	"cume_dist":  CUME_DIST,
	// value window functions
	"lag":         LAG,
	"lead":        LEAD,
	"first_value": FIRST_VALUE,
	"last_value":  LAST_VALUE,
	"nth_value":   NTH_VALUE,
	// builtin
	// whoever edit this, please follow the lexical order, or come up with a better ordering method
	// binary functions
	"endswith":    ENDSWITH,
	"findinset":   FINDINSET,
	"find_in_set": FINDINSET,
	"pow":         POW,
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
	"char":              CHAR,
	"concat_ws":         CONCAT_WS,
	"concat":            CONCAT,
	"current_timestamp": CURRENT_TIMESTAMP,
	"now":               CURRENT_TIMESTAMP,
	"localtime":         LOCALTIME,
	"localtimestamp":    LOCALTIME,
	"current_time":      CURRENT_TIME,
	"curtime":           CURRENT_TIME,
	"sysdate":           SYSDATE,
	"floor":             FLOOR,
	"lpad":              LPAD,
	"pi":                PI,
	"position":          POSITION,
	"quote":             QUOTE,
	"round":             ROUND,
	"rpad":              RPAD,
	"soundex":           SOUNDEX,
	"strcmp":            STRCMP,
	"substr":            SUBSTRING,
	"substring":         SUBSTRING,
	"mid":               SUBSTRING,
	"encode":            ENCODE,
	"utc_timestamp":     UTC_TIMESTAMP,
	"unix_timestamp":    UNIX_TIMESTAMP,
	"from_days":         FROM_DAYS,
	"from_unixtime":     FROM_UNIXTIME,
	"get_format":        GET_FORMAT,
	"left":              LEFT,
	"right":             RIGHT,
	// unary functions
	// whoever edit this, please follow the lexical order, or come up with a better ordering method
	"abs":                            ABS,
	"acos":                           ACOS,
	"aes_decrypt":                    AES_DECRYPT,
	"aes_encrypt":                    AES_ENCRYPT,
	"addtime":                        ADDTIME,
	"compress":                       COMPRESS,
	"uncompress":                     UNCOMPRESS,
	"uncompressed_length":            UNCOMPRESSED_LENGTH,
	"random_bytes":                   RANDOM_BYTES,
	"validate_password_strength":     VALIDATE_PASSWORD_STRENGTH,
	"inet6_aton":                     INET6_ATON,
	"inet6_ntoa":                     INET6_NTOA,
	"inet_aton":                      INET_ATON,
	"inet_ntoa":                      INET_NTOA,
	"is_ipv4":                        IS_IPV4,
	"is_ipv6":                        IS_IPV6,
	"is_ipv4_compat":                 IS_IPV4_COMPAT,
	"is_ipv4_mapped":                 IS_IPV4_MAPPED,
	"asin":                           ASIN,
	"assert":                         ASSERT,
	"bit_length":                     BIT_LENGTH,
	"date":                           DATE,
	"time":                           TIME,
	"time_format":                    TIME_FORMAT,
	"timestampadd":                   TIMESTAMPADD,
	"hour":                           HOUR,
	"microsecond":                    MICROSECOND,
	"minute":                         MINUTE,
	"quarter":                        QUARTER,
	"second":                         SECOND,
	"sqrt":                           SQRT,
	"to_seconds":                     TO_SECONDS,
	"day":                            DAY,
	"to_days":                        TO_DAYS,
	"dayofweek":                      DAYOFWEEK,
	"dayname":                        DAYNAME,
	"dayofmonth":                     DAYOFMONTH,
	"monthname":                      MONTHNAME,
	"dayofyear":                      DAYOFYEAR,
	"exp":                            EXP,
	"empty":                          EMPTY,
	"length":                         LENGTH,
	"octet_length":                   LENGTH,
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
	"radians":                        RADIANS,
	"rand":                           RANDOM,
	"reverse":                        REVERSE,
	"rtrim":                          RTRIM,
	"sign":                           SIGN,
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
	"degrees":                        DEGREES,
	"atan":                           ATAN,
	"atan2":                          ATAN2,
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
	"conv":                           CONV,
	"current_account_id":             CURRENT_ACCOUNT_ID,
	"current_account_name":           CURRENT_ACCOUNT_NAME,
	"current_role":                   CURRENT_ROLE,
	"current_role_id":                CURRENT_ROLE_ID,
	"current_role_name":              CURRENT_ROLE_NAME,
	"current_user_id":                CURRENT_USER_ID,
	"current_user_name":              CURRENT_USER_NAME,
	"found_rows":                     FOUND_ROWS,
	"greatest":                       GREATEST,
	"icu_version":                    ICULIBVERSION,
	"last_insert_id":                 LAST_INSERT_ID,
	"last_query_id":                  LAST_QUERY_ID,
	"last_uuid":                      LAST_QUERY_ID,
	"least":                          LEAST,
	"roles_graphml":                  ROLES_GRAPHML,
	"row_count":                      ROW_COUNT,
	"version":                        VERSION,
	"collation":                      COLLATION,
	"json_array":                     JSON_ARRAY,
	"json_extract":                   JSON_EXTRACT,
	"json_extract_string":            JSON_EXTRACT_STRING,
	"json_extract_float64":           JSON_EXTRACT_FLOAT64,
	"json_object":                    JSON_OBJECT,
	"json_arrayagg":                  JSON_ARRAYAGG,
	"json_objectagg":                 JSON_OBJECTAGG,
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
	"hash_partition":                 HASH_PARTITION,
	"mo_tuple_expr":                  MO_TUPLE_EXPR,
	"bin":                            BIN,
	"datediff":                       DATEDIFF,
	"timestampdiff":                  TIMESTAMPDIFF,
	"timediff":                       TIMEDIFF,
	"last_day":                       LAST_DAY,
	"makedate":                       MAKEDATE,
	"maketime":                       MAKETIME,
	"period_add":                     PERIOD_ADD,
	"period_diff":                    PERIOD_DIFF,
	"sec_to_time":                    SEC_TO_TIME,
	"subtime":                        SUBTIME,
	"time_to_sec":                    TIME_TO_SEC,
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
	"insert":                         INSERT,
	"instr":                          INSTR,
	"ord":                            ORD,
	"locate":                         LOCATE,
	"curdate":                        CURRENT_DATE,
	"current_date":                   CURRENT_DATE,
	"utc_date":                       UTC_DATE,
	"utc_time":                       UTC_TIME,
	"weekofyear":                     WEEKOFYEAR,
	"yearweek":                       YEARWEEK,
	"elt":                            ELT,
	"export_set":                     EXPORT_SET,
	"make_set":                       MAKE_SET,
	"ascii":                          ASCII,
	"replace":                        REPLACE,
	"mo_table_rows":                  MO_TABLE_ROWS,
	"mo_table_size":                  MO_TABLE_SIZE,
	"mo_table_col_max":               MO_TABLE_COL_MAX,
	"mo_table_col_min":               MO_TABLE_COL_MIN,
	"truncate":                       TRUNCATE,
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

	"mo_feature_registry_upsert": MO_FEATURE_REGISTRY_UPSERT,
	"mo_feature_limit_upsert":    MO_FEATURE_LIMIT_UPSERT,

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

	// llm function
	"llm_chat":      LLM_CHAT,
	"llm_embedding": LLM_EMBEDDING,

	// fault inject function
	"fault_inject": FAULT_INJECT,
}
