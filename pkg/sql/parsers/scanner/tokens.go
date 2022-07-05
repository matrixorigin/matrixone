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

package scanner

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

var keywords map[string]int

func initTokens(dialectType dialect.DialectType) {
	switch dialectType {
	case dialect.MYSQL:
		LEX_ERROR = MYSQL_LEX_ERROR
		PIPE_CONCAT = MYSQL_PIPE_CONCAT
		CONFIG = MYSQL_CONFIG
		SOME = MYSQL_SOME
		ANY = MYSQL_ANY
		UNKNOWN = MYSQL_UNKNOWN
		BOTH = MYSQL_BOTH
		LEADING = MYSQL_LEADING
		TRAILING = MYSQL_TRAILING
		SQL_BIG_RESULT = MYSQL_SQL_BIG_RESULT
		SQL_SMALL_RESULT = MYSQL_SQL_SMALL_RESULT
		SQL_BUFFER_RESULT = MYSQL_SQL_BUFFER_RESULT
		MIN = MYSQL_MIN
		MAX = MYSQL_MAX
		BIT_OR = MYSQL_BIT_OR
		BIT_AND = MYSQL_BIT_AND
		VERBOSE = MYSQL_VERBOSE
		SQL_TSI_MINUTE = MYSQL_SQL_TSI_MINUTE
		SQL_TSI_SECOND = MYSQL_SQL_TSI_SECOND
		SQL_TSI_YEAR = MYSQL_SQL_TSI_YEAR
		SQL_TSI_QUARTER = MYSQL_SQL_TSI_QUARTER
		SQL_TSI_MONTH = MYSQL_SQL_TSI_MONTH
		SQL_TSI_WEEK = MYSQL_SQL_TSI_WEEK
		SQL_TSI_DAY = MYSQL_SQL_TSI_DAY
		SQL_TSI_HOUR = MYSQL_SQL_TSI_HOUR
		YEAR_MONTH = MYSQL_YEAR_MONTH
		DAY_HOUR = MYSQL_DAY_HOUR
		DAY_MINUTE = MYSQL_DAY_MINUTE
		DAY_SECOND = MYSQL_DAY_SECOND
		DAY_MICROSECOND = MYSQL_DAY_MICROSECOND
		HOUR_MINUTE = MYSQL_HOUR_MINUTE
		HOUR_SECOND = MYSQL_HOUR_SECOND
		HOUR_MICROSECOND = MYSQL_HOUR_MICROSECOND
		MINUTE_SECOND = MYSQL_MINUTE_SECOND
		MINUTE_MICROSECOND = MYSQL_MINUTE_MICROSECOND
		SECOND_MICROSECOND = MYSQL_SECOND_MICROSECOND
		TYPE = MYSQL_TYPE
		ZONEMAP = MYSQL_ZONEMAP
		BSI = MYSQL_BSI
		ROW = MYSQL_ROW
		PROPERTIES = MYSQL_PROPERTIES
		INT1 = MYSQL_INT1
		INT2 = MYSQL_INT2
		INT3 = MYSQL_INT3
		INT4 = MYSQL_INT4
		INT8 = MYSQL_INT8
		LINES = MYSQL_LINES
		STARTING = MYSQL_STARTING
		TERMINATED = MYSQL_TERMINATED
		OPTIONALLY = MYSQL_OPTIONALLY
		ENCLOSED = MYSQL_ENCLOSED
		ESCAPED = MYSQL_ESCAPED
		LOAD = MYSQL_LOAD
		INFILE = MYSQL_INFILE
		OUTFILE = MYSQL_OUTFILE
		HEADER = MYSQL_HEADER
		MAX_FILE_SIZE = MYSQL_MAX_FILE_SIZE
		FORCE_QUOTE = MYSQL_FORCE_QUOTE
		AVG = MYSQL_AVG
		ADDDATE = MYSQL_ADDDATE
		COUNT = MYSQL_COUNT
		APPROX_COUNT_DISTINCT = MYSQL_APPROX_COUNT_DISTINCT
		CURDATE = MYSQL_CURDATE
		DATE_ADD = MYSQL_DATE_ADD
		DATE_SUB = MYSQL_DATE_SUB
		EXTRACT = MYSQL_EXTRACT
		MAX = MYSQL_MAX
		MID = MYSQL_MID
		NOW = MYSQL_NOW
		POSITION = MYSQL_POSITION
		SESSION_USER = MYSQL_SESSION_USER
		STD = MYSQL_STD
		STDDEV = MYSQL_STDDEV
		STDDEV_POP = MYSQL_STDDEV_POP
		STDDEV_SAMP = MYSQL_STDDEV_SAMP
		SUBDATE = MYSQL_SUBDATE
		SUM = MYSQL_SUM
		SYSTEM_USER = MYSQL_SYSTEM_USER
		TRANSLATE = MYSQL_TRANSLATE
		TRIM = MYSQL_TRIM
		VARIANCE = MYSQL_VARIANCE
		VAR_POP = MYSQL_VAR_POP
		VAR_SAMP = MYSQL_VAR_SAMP
		CURTIME = MYSQL_CURTIME
		SYSDATE = MYSQL_SYSDATE
		QUARTER = MYSQL_QUARTER
		REPEAT = MYSQL_REPEAT
		REVERSE = MYSQL_REVERSE
		ROW_COUNT = MYSQL_ROW_COUNT
		WEEK = MYSQL_WEEK
		ASCII = MYSQL_ASCII
		COALESCE = MYSQL_COALESCE
		COLLATION = MYSQL_COLLATION
		HOUR = MYSQL_HOUR
		MICROSECOND = MYSQL_MICROSECOND
		MINUTE = MYSQL_MINUTE
		MONTH = MYSQL_MONTH
		CURRENT_ROLE = MYSQL_CURRENT_ROLE
		CURRENT_USER = MYSQL_CURRENT_USER
		SECOND = MYSQL_SECOND
		SUBPARTITION = MYSQL_SUBPARTITION
		SUBPARTITIONS = MYSQL_SUBPARTITIONS
		PARTITIONS = MYSQL_PARTITIONS
		LINEAR = MYSQL_LINEAR
		ALGORITHM = MYSQL_ALGORITHM
		LIST = MYSQL_LIST
		RANGE = MYSQL_RANGE
		CHECK = MYSQL_CHECK
		ENFORCED = MYSQL_ENFORCED
		RESTRICT = MYSQL_RESTRICT
		CASCADE = MYSQL_CASCADE
		ACTION = MYSQL_ACTION
		PARTIAL = MYSQL_PARTIAL
		SIMPLE = MYSQL_SIMPLE
		AUTO_RANDOM = MYSQL_AUTO_RANDOM
		COLUMN_FORMAT = MYSQL_COLUMN_FORMAT
		CHECKSUM = MYSQL_CHECKSUM
		COMPRESSION = MYSQL_COMPRESSION
		COMPRESSED = MYSQL_COMPRESSED
		DATA = MYSQL_DATA
		DIRECTORY = MYSQL_DIRECTORY
		DELAY_KEY_WRITE = MYSQL_DELAY_KEY_WRITE
		ENCRYPTION = MYSQL_ENCRYPTION
		ENGINE = MYSQL_ENGINE
		MIN_ROWS = MYSQL_MIN_ROWS
		MAX_ROWS = MYSQL_MAX_ROWS
		PACK_KEYS = MYSQL_PACK_KEYS
		ROW_FORMAT = MYSQL_ROW_FORMAT
		STATS_AUTO_RECALC = MYSQL_STATS_AUTO_RECALC
		STATS_PERSISTENT = MYSQL_STATS_PERSISTENT
		STATS_SAMPLE_PAGES = MYSQL_STATS_SAMPLE_PAGES
		DYNAMIC = MYSQL_DYNAMIC
		FIXED = MYSQL_FIXED
		REDUNDANT = MYSQL_REDUNDANT
		COMPACT = MYSQL_COMPACT
		STORAGE = MYSQL_STORAGE
		DISK = MYSQL_DISK
		MEMORY = MYSQL_MEMORY
		AVG_ROW_LENGTH = MYSQL_AVG_ROW_LENGTH
		PROXY = MYSQL_PROXY
		FUNCTION = MYSQL_FUNCTION
		PRIVILEGES = MYSQL_PRIVILEGES
		TABLESPACE = MYSQL_TABLESPACE
		EXECUTE = MYSQL_EXECUTE
		SUPER = MYSQL_SUPER
		GRANT = MYSQL_GRANT
		OPTION = MYSQL_OPTION
		REFERENCES = MYSQL_REFERENCES
		REPLICATION = MYSQL_REPLICATION
		SLAVE = MYSQL_SLAVE
		CLIENT = MYSQL_CLIENT
		USAGE = MYSQL_USAGE
		RELOAD = MYSQL_RELOAD
		FILE = MYSQL_FILE
		TEMPORARY = MYSQL_TEMPORARY
		ROUTINE = MYSQL_ROUTINE
		EVENT = MYSQL_EVENT
		SHUTDOWN = MYSQL_SHUTDOWN
		REVOKE = MYSQL_REVOKE
		EXCEPT = MYSQL_EXCEPT
		LOCAL = MYSQL_LOCAL
		ASSIGNMENT = MYSQL_ASSIGNMENT
		NO = MYSQL_NO
		CHAIN = MYSQL_CHAIN
		RELEASE = MYSQL_RELEASE
		WORK = MYSQL_WORK
		CONSISTENT = MYSQL_CONSISTENT
		SNAPSHOT = MYSQL_SNAPSHOT
		CONNECTION = MYSQL_CONNECTION
		FORMAT = MYSQL_FORMAT
		EXPIRE = MYSQL_EXPIRE
		ACCOUNT = MYSQL_ACCOUNT
		UNLOCK = MYSQL_UNLOCK
		DAY = MYSQL_DAY
		NEVER = MYSQL_NEVER
		INDEXES = MYSQL_INDEXES
		WARNINGS = MYSQL_WARNINGS
		ERRORS = MYSQL_ERRORS
		OPEN = MYSQL_OPEN
		FIELDS = MYSQL_FIELDS
		COLUMNS = MYSQL_COLUMNS
		PASSWORD = MYSQL_PASSWORD
		MAX_QUERIES_PER_HOUR = MYSQL_MAX_QUERIES_PER_HOUR
		MAX_UPDATES_PER_HOUR = MYSQL_MAX_UPDATES_PER_HOUR
		MAX_CONNECTIONS_PER_HOUR = MYSQL_MAX_CONNECTIONS_PER_HOUR
		MAX_USER_CONNECTIONS = MYSQL_MAX_USER_CONNECTIONS
		CIPHER = MYSQL_CIPHER
		ISSUER = MYSQL_ISSUER
		X509 = MYSQL_X509
		SUBJECT = MYSQL_SUBJECT
		SAN = MYSQL_SAN
		REQUIRE = MYSQL_REQUIRE
		SSL = MYSQL_SSL
		NONE = MYSQL_NONE
		IDENTIFIED = MYSQL_IDENTIFIED
		USER = MYSQL_USER
		AT_ID = MYSQL_AT_ID
		AT_AT_ID = MYSQL_AT_AT_ID
		ROLE = MYSQL_ROLE
		PARSER = MYSQL_PARSER
		VISIBLE = MYSQL_VISIBLE
		INVISIBLE = MYSQL_INVISIBLE
		BTREE = MYSQL_BTREE
		HASH = MYSQL_HASH
		RTREE = MYSQL_RTREE
		UNION = MYSQL_UNION
		SELECT = MYSQL_SELECT
		STREAM = MYSQL_STREAM
		INSERT = MYSQL_INSERT
		UPDATE = MYSQL_UPDATE
		DELETE = MYSQL_DELETE
		FROM = MYSQL_FROM
		WHERE = MYSQL_WHERE
		GROUP = MYSQL_GROUP
		HAVING = MYSQL_HAVING
		ORDER = MYSQL_ORDER
		BY = MYSQL_BY
		LIMIT = MYSQL_LIMIT
		OFFSET = MYSQL_OFFSET
		FOR = MYSQL_FOR
		ALL = MYSQL_ALL
		DISTINCT = MYSQL_DISTINCT
		AS = MYSQL_AS
		EXISTS = MYSQL_EXISTS
		ASC = MYSQL_ASC
		DESC = MYSQL_DESC
		INTO = MYSQL_INTO
		DUPLICATE = MYSQL_DUPLICATE
		KEY = MYSQL_KEY
		DEFAULT = MYSQL_DEFAULT
		SET = MYSQL_SET
		LOCK = MYSQL_LOCK
		KEYS = MYSQL_KEYS
		VALUES = MYSQL_VALUES
		NEXT = MYSQL_NEXT
		VALUE = MYSQL_VALUE
		SHARE = MYSQL_SHARE
		MODE = MYSQL_MODE
		SQL_NO_CACHE = MYSQL_SQL_NO_CACHE
		SQL_CACHE = MYSQL_SQL_CACHE
		JOIN = MYSQL_JOIN
		STRAIGHT_JOIN = MYSQL_STRAIGHT_JOIN
		LEFT = MYSQL_LEFT
		RIGHT = MYSQL_RIGHT
		INNER = MYSQL_INNER
		OUTER = MYSQL_OUTER
		CROSS = MYSQL_CROSS
		NATURAL = MYSQL_NATURAL
		USE = MYSQL_USE
		FORCE = MYSQL_FORCE
		ON = MYSQL_ON
		USING = MYSQL_USING
		ID = MYSQL_ID
		HEX = MYSQL_HEX
		STRING = MYSQL_STRING
		INTEGRAL = MYSQL_INTEGRAL
		FLOAT = MYSQL_FLOAT
		HEXNUM = MYSQL_HEXNUM
		VALUE_ARG = MYSQL_VALUE_ARG
		LIST_ARG = MYSQL_LIST_ARG
		COMMENT = MYSQL_COMMENT
		COMMENT_KEYWORD = MYSQL_COMMENT_KEYWORD
		BIT_LITERAL = MYSQL_BIT_LITERAL
		NULL = MYSQL_NULL
		TRUE = MYSQL_TRUE
		FALSE = MYSQL_FALSE
		OR = MYSQL_OR
		XOR = MYSQL_XOR
		AND = MYSQL_AND
		NOT = MYSQL_NOT
		BETWEEN = MYSQL_BETWEEN
		CASE = MYSQL_CASE
		WHEN = MYSQL_WHEN
		THEN = MYSQL_THEN
		ELSE = MYSQL_ELSE
		END = MYSQL_END
		LE = MYSQL_LE
		GE = MYSQL_GE
		NE = MYSQL_NE
		NULL_SAFE_EQUAL = MYSQL_NULL_SAFE_EQUAL
		IS = MYSQL_IS
		LIKE = MYSQL_LIKE
		REGEXP = MYSQL_REGEXP
		IN = MYSQL_IN
		SHIFT_LEFT = MYSQL_SHIFT_LEFT
		SHIFT_RIGHT = MYSQL_SHIFT_RIGHT
		DIV = MYSQL_DIV
		MOD = MYSQL_MOD
		UNARY = MYSQL_UNARY
		COLLATE = MYSQL_COLLATE
		BINARY = MYSQL_BINARY
		UNDERSCORE_BINARY = MYSQL_UNDERSCORE_BINARY
		INTERVAL = MYSQL_INTERVAL
		BEGIN = MYSQL_BEGIN
		START = MYSQL_START
		TRANSACTION = MYSQL_TRANSACTION
		COMMIT = MYSQL_COMMIT
		ROLLBACK = MYSQL_ROLLBACK
		BIT = MYSQL_BIT
		TINYINT = MYSQL_TINYINT
		SMALLINT = MYSQL_SMALLINT
		MEDIUMINT = MYSQL_MEDIUMINT
		INT = MYSQL_INT
		INTEGER = MYSQL_INTEGER
		BIGINT = MYSQL_BIGINT
		INTNUM = MYSQL_INTNUM
		REAL = MYSQL_REAL
		DOUBLE = MYSQL_DOUBLE
		FLOAT_TYPE = MYSQL_FLOAT_TYPE
		DECIMAL = MYSQL_DECIMAL
		NUMERIC = MYSQL_NUMERIC
		TIME = MYSQL_TIME
		TIMESTAMP = MYSQL_TIMESTAMP
		DATETIME = MYSQL_DATETIME
		YEAR = MYSQL_YEAR
		CHAR = MYSQL_CHAR
		VARCHAR = MYSQL_VARCHAR
		BOOL = MYSQL_BOOL
		CHARACTER = MYSQL_CHARACTER
		VARBINARY = MYSQL_VARBINARY
		NCHAR = MYSQL_NCHAR
		TEXT = MYSQL_TEXT
		TINYTEXT = MYSQL_TINYTEXT
		MEDIUMTEXT = MYSQL_MEDIUMTEXT
		LONGTEXT = MYSQL_LONGTEXT
		BLOB = MYSQL_BLOB
		TINYBLOB = MYSQL_TINYBLOB
		MEDIUMBLOB = MYSQL_MEDIUMBLOB
		LONGBLOB = MYSQL_LONGBLOB
		JSON = MYSQL_JSON
		ENUM = MYSQL_ENUM
		GEOMETRY = MYSQL_GEOMETRY
		POINT = MYSQL_POINT
		LINESTRING = MYSQL_LINESTRING
		POLYGON = MYSQL_POLYGON
		GEOMETRYCOLLECTION = MYSQL_GEOMETRYCOLLECTION
		MULTIPOINT = MYSQL_MULTIPOINT
		MULTILINESTRING = MYSQL_MULTILINESTRING
		MULTIPOLYGON = MYSQL_MULTIPOLYGON
		CREATE = MYSQL_CREATE
		ALTER = MYSQL_ALTER
		DROP = MYSQL_DROP
		RENAME = MYSQL_RENAME
		ANALYZE = MYSQL_ANALYZE
		ADD = MYSQL_ADD
		SCHEMA = MYSQL_SCHEMA
		TABLE = MYSQL_TABLE
		INDEX = MYSQL_INDEX
		VIEW = MYSQL_VIEW
		TO = MYSQL_TO
		IGNORE = MYSQL_IGNORE
		IF = MYSQL_IF
		UNIQUE = MYSQL_UNIQUE
		PRIMARY = MYSQL_PRIMARY
		COLUMN = MYSQL_COLUMN
		CONSTRAINT = MYSQL_CONSTRAINT
		SPATIAL = MYSQL_SPATIAL
		FULLTEXT = MYSQL_FULLTEXT
		FOREIGN = MYSQL_FOREIGN
		KEY_BLOCK_SIZE = MYSQL_KEY_BLOCK_SIZE
		SHOW = MYSQL_SHOW
		DESCRIBE = MYSQL_DESCRIBE
		EXPLAIN = MYSQL_EXPLAIN
		DATE = MYSQL_DATE
		ESCAPE = MYSQL_ESCAPE
		REPAIR = MYSQL_REPAIR
		OPTIMIZE = MYSQL_OPTIMIZE
		TRUNCATE = MYSQL_TRUNCATE
		MAXVALUE = MYSQL_MAXVALUE
		PARTITION = MYSQL_PARTITION
		REORGANIZE = MYSQL_REORGANIZE
		LESS = MYSQL_LESS
		THAN = MYSQL_THAN
		PROCEDURE = MYSQL_PROCEDURE
		TRIGGER = MYSQL_TRIGGER
		STATUS = MYSQL_STATUS
		VARIABLES = MYSQL_VARIABLES
		NULLX = MYSQL_NULLX
		AUTO_INCREMENT = MYSQL_AUTO_INCREMENT
		APPROXNUM = MYSQL_APPROXNUM
		SIGNED = MYSQL_SIGNED
		UNSIGNED = MYSQL_UNSIGNED
		ZEROFILL = MYSQL_ZEROFILL
		DATABASES = MYSQL_DATABASES
		TABLES = MYSQL_TABLES
		EXTENDED = MYSQL_EXTENDED
		FULL = MYSQL_FULL
		PROCESSLIST = MYSQL_PROCESSLIST
		NAMES = MYSQL_NAMES
		CHARSET = MYSQL_CHARSET
		GLOBAL = MYSQL_GLOBAL
		SESSION = MYSQL_SESSION
		ISOLATION = MYSQL_ISOLATION
		LEVEL = MYSQL_LEVEL
		READ = MYSQL_READ
		WRITE = MYSQL_WRITE
		ONLY = MYSQL_ONLY
		REPEATABLE = MYSQL_REPEATABLE
		COMMITTED = MYSQL_COMMITTED
		UNCOMMITTED = MYSQL_UNCOMMITTED
		SERIALIZABLE = MYSQL_SERIALIZABLE
		CURRENT_TIMESTAMP = MYSQL_CURRENT_TIMESTAMP
		DATABASE = MYSQL_DATABASE
		CURRENT_DATE = MYSQL_CURRENT_DATE
		CURRENT_TIME = MYSQL_CURRENT_TIME
		LOCALTIME = MYSQL_LOCALTIME
		LOCALTIMESTAMP = MYSQL_LOCALTIMESTAMP
		UTC_DATE = MYSQL_UTC_DATE
		UTC_TIME = MYSQL_UTC_TIME
		UTC_TIMESTAMP = MYSQL_UTC_TIMESTAMP
		REPLACE = MYSQL_REPLACE
		CONVERT = MYSQL_CONVERT
		CAST = MYSQL_CAST
		SUBSTR = MYSQL_SUBSTR
		SUBSTRING = MYSQL_SUBSTRING
		GROUP_CONCAT = MYSQL_GROUP_CONCAT
		SEPARATOR = MYSQL_SEPARATOR
		MATCH = MYSQL_MATCH
		AGAINST = MYSQL_AGAINST
		BOOLEAN = MYSQL_BOOLEAN
		LANGUAGE = MYSQL_LANGUAGE
		WITH = MYSQL_WITH
		QUERY = MYSQL_QUERY
		EXPANSION = MYSQL_EXPANSION
		UNUSED = MYSQL_UNUSED

	case dialect.POSTGRESQL:
		USE = POSTGRESQL_USE
		ID = POSTGRESQL_ID
		INTEGRAL = POSTGRESQL_INTEGRAL
		FLOAT = POSTGRESQL_FLOAT
		HEX = POSTGRESQL_HEX
		HEXNUM = POSTGRESQL_HEXNUM
		BIT_LITERAL = POSTGRESQL_BIT_LITERAL
	}
	keywords = map[string]int{
		"accessible":               UNUSED,
		"account":                  ACCOUNT,
		"add":                      ADD,
		"action":                   ACTION,
		"against":                  AGAINST,
		"all":                      ALL,
		"alter":                    ALTER,
		"algorithm":                ALGORITHM,
		"analyze":                  ANALYZE,
		"and":                      AND,
		"any":                      ANY,
		"as":                       AS,
		"asc":                      ASC,
		"ascii":                    ASCII,
		"asensitive":               UNUSED,
		"auto_increment":           AUTO_INCREMENT,
		"auto_random":              AUTO_RANDOM,
		"avg_row_length":           AVG_ROW_LENGTH,
		"avg":                      AVG,
		"bsi":                      BSI,
		"before":                   UNUSED,
		"begin":                    BEGIN,
		"between":                  BETWEEN,
		"bigint":                   BIGINT,
		"binary":                   BINARY,
		"_binary":                  UNDERSCORE_BINARY,
		"bit":                      BIT,
		"blob":                     BLOB,
		"bool":                     BOOL,
		"boolean":                  BOOLEAN,
		"both":                     BOTH,
		"by":                       BY,
		"btree":                    BTREE,
		"bit_or":                   BIT_OR,
		"bit_and":                  BIT_AND,
		"call":                     UNUSED,
		"cascade":                  CASCADE,
		"case":                     CASE,
		"cast":                     CAST,
		"change":                   UNUSED,
		"char":                     CHAR,
		"character":                CHARACTER,
		"charset":                  CHARSET,
		"check":                    CHECK,
		"checksum":                 CHECKSUM,
		"coalesce":                 COALESCE,
		"compressed":               COMPRESSED,
		"compression":              COMPRESSION,
		"collate":                  COLLATE,
		"collation":                COLLATION,
		"column":                   COLUMN,
		"columns":                  COLUMNS,
		"column_format":            COLUMN_FORMAT,
		"comment":                  COMMENT_KEYWORD,
		"committed":                COMMITTED,
		"commit":                   COMMIT,
		"compact":                  COMPACT,
		"condition":                UNUSED,
		"constraint":               CONSTRAINT,
		"consistent":               CONSISTENT,
		"continue":                 UNUSED,
		"connection":               CONNECTION,
		"convert":                  CONVERT,
		"config":                   CONFIG,
		"cipher":                   CIPHER,
		"chain":                    CHAIN,
		"client":                   CLIENT,
		"san":                      SAN,
		"substr":                   SUBSTR,
		"substring":                SUBSTRING,
		"subject":                  SUBJECT,
		"subpartition":             SUBPARTITION,
		"subpartitions":            SUBPARTITIONS,
		"snapshot":                 SNAPSHOT,
		"sysdate":                  SYSDATE,
		"create":                   CREATE,
		"cross":                    CROSS,
		"current_date":             CURRENT_DATE,
		"current_time":             CURRENT_TIME,
		"current_timestamp":        CURRENT_TIMESTAMP,
		"current_user":             CURRENT_USER,
		"current_role":             CURRENT_ROLE,
		"curtime":                  CURTIME,
		"cursor":                   UNUSED,
		"database":                 DATABASE,
		"databases":                DATABASES,
		"day":                      DAY,
		"date":                     DATE,
		"data":                     DATA,
		"datetime":                 DATETIME,
		"dec":                      UNUSED,
		"decimal":                  DECIMAL,
		"declare":                  UNUSED,
		"default":                  DEFAULT,
		"delayed":                  UNUSED,
		"delete":                   DELETE,
		"desc":                     DESC,
		"describe":                 DESCRIBE,
		"deterministic":            UNUSED,
		"distinct":                 DISTINCT,
		"distinctrow":              UNUSED,
		"disk":                     DISK,
		"div":                      DIV,
		"directory":                DIRECTORY,
		"double":                   DOUBLE,
		"drop":                     DROP,
		"dynamic":                  DYNAMIC,
		"duplicate":                DUPLICATE,
		"each":                     UNUSED,
		"else":                     ELSE,
		"elseif":                   UNUSED,
		"enclosed":                 ENCLOSED,
		"encryption":               ENCRYPTION,
		"engine":                   ENGINE,
		"end":                      END,
		"enum":                     ENUM,
		"enforced":                 ENFORCED,
		"escape":                   ESCAPE,
		"escaped":                  ESCAPED,
		"exists":                   EXISTS,
		"exit":                     UNUSED,
		"explain":                  EXPLAIN,
		"expansion":                EXPANSION,
		"extended":                 EXTENDED,
		"expire":                   EXPIRE,
		"except":                   EXCEPT,
		"execute":                  EXECUTE,
		"errors":                   ERRORS,
		"event":                    EVENT,
		"false":                    FALSE,
		"fetch":                    UNUSED,
		"float":                    FLOAT_TYPE,
		"float4":                   UNUSED,
		"float8":                   UNUSED,
		"for":                      FOR,
		"force":                    FORCE,
		"foreign":                  FOREIGN,
		"format":                   FORMAT,
		"from":                     FROM,
		"full":                     FULL,
		"fulltext":                 FULLTEXT,
		"function":                 FUNCTION,
		"fields":                   FIELDS,
		"file":                     FILE,
		"fixed":                    FIXED,
		"generated":                UNUSED,
		"geometry":                 GEOMETRY,
		"geometrycollection":       GEOMETRYCOLLECTION,
		"get":                      UNUSED,
		"global":                   GLOBAL,
		"grant":                    GRANT,
		"group":                    GROUP,
		"group_concat":             GROUP_CONCAT,
		"having":                   HAVING,
		"hash":                     HASH,
		"high_priority":            UNUSED,
		"hour":                     HOUR,
		"identified":               IDENTIFIED,
		"if":                       IF,
		"ignore":                   IGNORE,
		"in":                       IN,
		"index":                    INDEX,
		"indexes":                  INDEXES,
		"infile":                   INFILE,
		"inout":                    UNUSED,
		"inner":                    INNER,
		"insensitive":              UNUSED,
		"insert":                   INSERT,
		"int":                      INT,
		"int1":                     INT1,
		"int2":                     INT2,
		"int3":                     INT3,
		"int4":                     INT4,
		"int8":                     INT8,
		"integer":                  INTEGER,
		"interval":                 INTERVAL,
		"into":                     INTO,
		"invisible":                INVISIBLE,
		"io_after_gtids":           UNUSED,
		"is":                       IS,
		"issuer":                   ISSUER,
		"isolation":                ISOLATION,
		"iterate":                  UNUSED,
		"join":                     JOIN,
		"json":                     JSON,
		"key":                      KEY,
		"keys":                     KEYS,
		"key_block_size":           KEY_BLOCK_SIZE,
		"kill":                     UNUSED,
		"language":                 LANGUAGE,
		"leading":                  LEADING,
		"leave":                    UNUSED,
		"left":                     LEFT,
		"less":                     LESS,
		"level":                    LEVEL,
		"like":                     LIKE,
		"list":                     LIST,
		"limit":                    LIMIT,
		"linear":                   LINEAR,
		"lines":                    LINES,
		"linestring":               LINESTRING,
		"load":                     LOAD,
		"localtime":                LOCALTIME,
		"localtimestamp":           LOCALTIMESTAMP,
		"lock":                     LOCK,
		"long":                     UNUSED,
		"longblob":                 LONGBLOB,
		"longtext":                 LONGTEXT,
		"loop":                     UNUSED,
		"low_priority":             UNUSED,
		"local":                    LOCAL,
		"master_bind":              UNUSED,
		"match":                    MATCH,
		"maxvalue":                 MAXVALUE,
		"mediumblob":               MEDIUMBLOB,
		"mediumint":                MEDIUMINT,
		"mediumtext":               MEDIUMTEXT,
		"middleint":                UNUSED,
		"minute":                   MINUTE,
		"microsecond":              MICROSECOND,
		"mod":                      MOD,
		"month":                    MONTH,
		"mode":                     MODE,
		"memory":                   MEMORY,
		"modifies":                 UNUSED,
		"multilinestring":          MULTILINESTRING,
		"multipoint":               MULTIPOINT,
		"multipolygon":             MULTIPOLYGON,
		"max_queries_per_hour":     MAX_QUERIES_PER_HOUR,
		"max_update_per_hour":      MAX_UPDATES_PER_HOUR,
		"max_connections_per_hour": MAX_CONNECTIONS_PER_HOUR,
		"max_user_connections":     MAX_USER_CONNECTIONS,
		"max_rows":                 MAX_ROWS,
		"min_rows":                 MIN_ROWS,
		"names":                    NAMES,
		"natural":                  NATURAL,
		"nchar":                    NCHAR,
		"next":                     NEXT,
		"never":                    NEVER,
		"not":                      NOT,
		"no":                       NO,
		"no_write_to_binlog":       UNUSED,
		"null":                     NULL,
		"numeric":                  NUMERIC,
		"none":                     NONE,
		"offset":                   OFFSET,
		"on":                       ON,
		"only":                     ONLY,
		"optimize":                 OPTIMIZE,
		"optimizer_costs":          UNUSED,
		"option":                   OPTION,
		"optionally":               OPTIONALLY,
		"open":                     OPEN,
		"or":                       OR,
		"order":                    ORDER,
		"out":                      UNUSED,
		"outer":                    OUTER,
		"outfile":                  OUTFILE,
		"header":                   HEADER,
		"max_file_size":            MAX_FILE_SIZE,
		"force_quote":              FORCE_QUOTE,
		"parser":                   PARSER,
		"partition":                PARTITION,
		"partitions":               PARTITIONS,
		"partial":                  PARTIAL,
		"password":                 PASSWORD,
		"pack_keys":                PACK_KEYS,
		"point":                    POINT,
		"polygon":                  POLYGON,
		"precision":                UNUSED,
		"primary":                  PRIMARY,
		"processlist":              PROCESSLIST,
		"procedure":                PROCEDURE,
		"proxy":                    PROXY,
		"properties":               PROPERTIES,
		"privileges":               PRIVILEGES,
		"query":                    QUERY,
		"quarter":                  QUARTER,
		"range":                    RANGE,
		"read":                     READ,
		"reads":                    UNUSED,
		"redundant":                REDUNDANT,
		"read_write":               UNUSED,
		"real":                     REAL,
		"references":               REFERENCES,
		"regexp":                   REGEXP,
		"release":                  RELEASE,
		"rename":                   RENAME,
		"reorganize":               REORGANIZE,
		"repair":                   REPAIR,
		"repeat":                   REPEAT,
		"repeatable":               REPEATABLE,
		"replace":                  REPLACE,
		"replication":              REPLICATION,
		"require":                  REQUIRE,
		"resignal":                 UNUSED,
		"restrict":                 RESTRICT,
		"return":                   UNUSED,
		"revoke":                   REVOKE,
		"reverse":                  REVERSE,
		"reload":                   RELOAD,
		"right":                    RIGHT,
		"rlike":                    REGEXP,
		"rollback":                 ROLLBACK,
		"role":                     ROLE,
		"routine":                  ROUTINE,
		"row":                      ROW,
		"row_format":               ROW_FORMAT,
		"row_count":                ROW_COUNT,
		"rtree":                    RTREE,
		"schema":                   SCHEMA,
		"schemas":                  UNUSED,
		"second":                   SECOND,
		"select":                   SELECT,
		"sensitive":                UNUSED,
		"separator":                SEPARATOR,
		"serializable":             SERIALIZABLE,
		"session":                  SESSION,
		"set":                      SET,
		"share":                    SHARE,
		"show":                     SHOW,
		"shutdown":                 SHUTDOWN,
		"signal":                   UNUSED,
		"signed":                   SIGNED,
		"simple":                   SIMPLE,
		"smallint":                 SMALLINT,
		"spatial":                  SPATIAL,
		"specific":                 UNUSED,
		"sql":                      UNUSED,
		"sqlexception":             UNUSED,
		"sqlstate":                 UNUSED,
		"sqlwarning":               UNUSED,
		"sql_big_result":           SQL_BIG_RESULT,
		"sql_cache":                SQL_CACHE,
		"sql_calc_found_rows":      UNUSED,
		"sql_no_cache":             SQL_NO_CACHE,
		"sql_small_result":         SQL_SMALL_RESULT,
		"sql_buffer_result":        SQL_BUFFER_RESULT,
		"ssl":                      SSL,
		"slave":                    SLAVE,
		"start":                    START,
		"starting":                 STARTING,
		"status":                   STATUS,
		"stats_auto_recalc":        STATS_AUTO_RECALC,
		"stats_persistent":         STATS_PERSISTENT,
		"stats_sample_pages":       STATS_SAMPLE_PAGES,
		"stored":                   UNUSED,
		"storage":                  STORAGE,
		"straight_join":            STRAIGHT_JOIN,
		"stream":                   STREAM,
		"super":                    SUPER,
		"table":                    TABLE,
		"tables":                   TABLES,
		"tablespace":               TABLESPACE,
		"terminated":               TERMINATED,
		"text":                     TEXT,
		"temporary":                TEMPORARY,
		"than":                     THAN,
		"then":                     THEN,
		"time":                     TIME,
		"timestamp":                TIMESTAMP,
		"tinyblob":                 TINYBLOB,
		"tinyint":                  TINYINT,
		"tinytext":                 TINYTEXT,
		"to":                       TO,
		"trailing":                 TRAILING,
		"transaction":              TRANSACTION,
		"trigger":                  TRIGGER,
		"true":                     TRUE,
		"truncate":                 TRUNCATE,
		"uncommitted":              UNCOMMITTED,
		"undo":                     UNUSED,
		"unknown":                  UNKNOWN,
		"union":                    UNION,
		"unique":                   UNIQUE,
		"unlock":                   UNLOCK,
		"unsigned":                 UNSIGNED,
		"update":                   UPDATE,
		"usage":                    USAGE,
		"use":                      USE,
		"user":                     USER,
		"using":                    USING,
		"utc_date":                 UTC_DATE,
		"utc_time":                 UTC_TIME,
		"utc_timestamp":            UTC_TIMESTAMP,
		"values":                   VALUES,
		"variables":                VARIABLES,
		"varbinary":                VARBINARY,
		"varchar":                  VARCHAR,
		"varcharacter":             UNUSED,
		"varying":                  UNUSED,
		"virtual":                  UNUSED,
		"view":                     VIEW,
		"visible":                  VISIBLE,
		"week":                     WEEK,
		"when":                     WHEN,
		"where":                    WHERE,
		"while":                    UNUSED,
		"with":                     WITH,
		"write":                    WRITE,
		"warnings":                 WARNINGS,
		"work":                     WORK,
		"xor":                      XOR,
		"x509":                     X509,
		"year":                     YEAR,
		"zerofill":                 ZEROFILL,
		"zonemap":                  ZONEMAP,
		"adddate":                  ADDDATE,
		"count":                    COUNT,
		"approx_count_distinct":    APPROX_COUNT_DISTINCT,
		"approx_percentile":        APPROX_PERCENTILE,
		"curdate":                  CURDATE,
		"date_add":                 DATE_ADD,
		"date_sub":                 DATE_SUB,
		"extract":                  EXTRACT,
		"max":                      MAX,
		"mid":                      MID,
		"now":                      NOW,
		"position":                 POSITION,
		"session_user":             SESSION_USER,
		"std":                      STD,
		"stddev":                   STDDEV,
		"stddev_pop":               STDDEV_POP,
		"stddev_samp":              STDDEV_SAMP,
		"subdate":                  SUBDATE,
		"sum":                      SUM,
		"system_user":              SYSTEM_USER,
		"some":                     SOME,
		"translate":                TRANSLATE,
		"trim":                     TRIM,
		"variance":                 VARIANCE,
		"var_pop":                  VAR_POP,
		"var_samp":                 VAR_SAMP,
		"type":                     TYPE,
		"verbose":                  VERBOSE,
		"sql_tsi_minute":           SQL_TSI_MINUTE,
		"sql_tsi_second":           SQL_TSI_SECOND,
		"sql_tsi_year":             SQL_TSI_YEAR,
		"sql_tsi_quarter":          SQL_TSI_QUARTER,
		"sql_tsi_month":            SQL_TSI_MONTH,
		"sql_tsi_week":             SQL_TSI_WEEK,
		"sql_tsi_day":              SQL_TSI_DAY,
		"sql_tsi_hour":             SQL_TSI_HOUR,
		"year_month":               YEAR_MONTH,
		"day_hour":                 DAY_HOUR,
		"day_minute":               DAY_MINUTE,
		"day_second":               DAY_SECOND,
		"day_microsecond":          DAY_MICROSECOND,
		"hour_minute":              HOUR_MINUTE,
		"hour_second":              HOUR_SECOND,
		"hour_microsecond":         HOUR_MICROSECOND,
		"minute_second":            MINUTE_SECOND,
		"minute_microsecond":       MINUTE_MICROSECOND,
		"min":                      MIN,
		"second_microsecond":       SECOND_MICROSECOND,
	}
}

// mysql
var (
	PIPE_CONCAT              int
	CONFIG                   int
	SOME                     int
	ANY                      int
	UNKNOWN                  int
	TRAILING                 int
	LEADING                  int
	BOTH                     int
	SQL_SMALL_RESULT         int
	SQL_BIG_RESULT           int
	SQL_BUFFER_RESULT        int
	BIT_OR                   int
	BIT_AND                  int
	SQL_TSI_MINUTE           int
	SQL_TSI_SECOND           int
	SQL_TSI_YEAR             int
	SQL_TSI_QUARTER          int
	SQL_TSI_MONTH            int
	SQL_TSI_WEEK             int
	SQL_TSI_DAY              int
	SQL_TSI_HOUR             int
	YEAR_MONTH               int
	DAY_HOUR                 int
	DAY_MINUTE               int
	DAY_SECOND               int
	DAY_MICROSECOND          int
	HOUR_MINUTE              int
	HOUR_SECOND              int
	HOUR_MICROSECOND         int
	MINUTE_SECOND            int
	MINUTE_MICROSECOND       int
	SECOND_MICROSECOND       int
	TYPE                     int
	ZONEMAP                  int
	BSI                      int
	ROW                      int
	PROPERTIES               int
	TERMINATED               int
	ENCLOSED                 int
	OPTIONALLY               int
	ESCAPED                  int
	ADDDATE                  int
	COUNT                    int
	APPROX_COUNT_DISTINCT    int
	APPROX_PERCENTILE        int
	CURDATE                  int
	DATE_ADD                 int
	DATE_SUB                 int
	EXTRACT                  int
	MAX                      int
	MID                      int
	MIN                      int
	NOW                      int
	POSITION                 int
	SESSION_USER             int
	STD                      int
	STDDEV                   int
	STDDEV_POP               int
	STDDEV_SAMP              int
	SUBDATE                  int
	SUM                      int
	SYSTEM_USER              int
	TRANSLATE                int
	TRIM                     int
	VARIANCE                 int
	VAR_POP                  int
	VAR_SAMP                 int
	LEX_ERROR                int
	COLLATION                int
	HOUR                     int
	MICROSECOND              int
	MINUTE                   int
	QUARTER                  int
	REPEAT                   int
	REVERSE                  int
	ROW_COUNT                int
	WEEK                     int
	ASCII                    int
	COALESCE                 int
	SECOND                   int
	SUBPARTITIONS            int
	SUBPARTITION             int
	PARTITIONS               int
	LINEAR                   int
	ALGORITHM                int
	LIST                     int
	RANGE                    int
	CHECK                    int
	ENFORCED                 int
	RESTRICT                 int
	CASCADE                  int
	ACTION                   int
	PARTIAL                  int
	SIMPLE                   int
	AUTO_RANDOM              int
	COLUMN_FORMAT            int
	CHECKSUM                 int
	COMPRESSION              int
	DATA                     int
	DIRECTORY                int
	DELAY_KEY_WRITE          int
	ENCRYPTION               int
	ENGINE                   int
	MAX_ROWS                 int
	MIN_ROWS                 int
	PACK_KEYS                int
	ROW_FORMAT               int
	STATS_AUTO_RECALC        int
	STATS_PERSISTENT         int
	STATS_SAMPLE_PAGES       int
	DYNAMIC                  int
	FIXED                    int
	COMPRESSED               int
	REDUNDANT                int
	COMPACT                  int
	STORAGE                  int
	DISK                     int
	MEMORY                   int
	AVG_ROW_LENGTH           int
	PROXY                    int
	FILE                     int
	TEMPORARY                int
	ROUTINE                  int
	EVENT                    int
	SHUTDOWN                 int
	RELOAD                   int
	FUNCTION                 int
	PRIVILEGES               int
	TABLESPACE               int
	EXECUTE                  int
	SUPER                    int
	GRANT                    int
	OPTION                   int
	REFERENCES               int
	REPLICATION              int
	SLAVE                    int
	CLIENT                   int
	USAGE                    int
	REVOKE                   int
	EXCEPT                   int
	LOCAL                    int
	ASSIGNMENT               int
	RELEASE                  int
	NO                       int
	CHAIN                    int
	WORK                     int
	CONSISTENT               int
	SNAPSHOT                 int
	CONNECTION               int
	FORMAT                   int
	EXPIRE                   int
	ACCOUNT                  int
	UNLOCK                   int
	DAY                      int
	NEVER                    int
	INDEXES                  int
	WARNINGS                 int
	ERRORS                   int
	OPEN                     int
	FIELDS                   int
	COLUMNS                  int
	PASSWORD                 int
	MAX_QUERIES_PER_HOUR     int
	MAX_UPDATES_PER_HOUR     int
	MAX_CONNECTIONS_PER_HOUR int
	MAX_USER_CONNECTIONS     int
	CIPHER                   int
	ISSUER                   int
	X509                     int
	SUBJECT                  int
	SAN                      int
	REQUIRE                  int
	SSL                      int
	NONE                     int
	IDENTIFIED               int
	USER                     int
	ROLE                     int
	PARSER                   int
	VISIBLE                  int
	INVISIBLE                int
	BTREE                    int
	HASH                     int
	RTREE                    int
	AT_ID                    int
	AT_AT_ID                 int
	UNION                    int
	SELECT                   int
	STREAM                   int
	INSERT                   int
	UPDATE                   int
	DELETE                   int
	FROM                     int
	WHERE                    int
	GROUP                    int
	HAVING                   int
	ORDER                    int
	BY                       int
	LIMIT                    int
	OFFSET                   int
	FOR                      int
	ALL                      int
	DISTINCT                 int
	AS                       int
	EXISTS                   int
	ASC                      int
	DESC                     int
	INTO                     int
	DUPLICATE                int
	KEY                      int
	DEFAULT                  int
	SET                      int
	LOCK                     int
	KEYS                     int
	VALUES                   int
	NEXT                     int
	VALUE                    int
	SHARE                    int
	MODE                     int
	SQL_NO_CACHE             int
	SQL_CACHE                int
	JOIN                     int
	STRAIGHT_JOIN            int
	LEFT                     int
	RIGHT                    int
	INNER                    int
	OUTER                    int
	CROSS                    int
	NATURAL                  int
	USE                      int
	FORCE                    int
	ON                       int
	USING                    int
	ID                       int
	HEX                      int
	STRING                   int
	INTEGRAL                 int
	FLOAT                    int
	HEXNUM                   int
	VALUE_ARG                int
	LIST_ARG                 int
	COMMENT                  int
	COMMENT_KEYWORD          int
	BIT_LITERAL              int
	NULL                     int
	TRUE                     int
	FALSE                    int
	OR                       int
	XOR                      int
	AND                      int
	NOT                      int
	BETWEEN                  int
	CASE                     int
	WHEN                     int
	THEN                     int
	ELSE                     int
	END                      int
	LE                       int
	GE                       int
	NE                       int
	NULL_SAFE_EQUAL          int
	IS                       int
	LIKE                     int
	REGEXP                   int
	IN                       int
	SHIFT_LEFT               int
	SHIFT_RIGHT              int
	DIV                      int
	MOD                      int
	UNARY                    int
	COLLATE                  int
	BINARY                   int
	UNDERSCORE_BINARY        int
	INTERVAL                 int
	BEGIN                    int
	START                    int
	TRANSACTION              int
	COMMIT                   int
	ROLLBACK                 int
	BIT                      int
	TINYINT                  int
	SMALLINT                 int
	MEDIUMINT                int
	INT                      int
	INTEGER                  int
	BIGINT                   int
	INTNUM                   int
	REAL                     int
	DOUBLE                   int
	FLOAT_TYPE               int
	DECIMAL                  int
	NUMERIC                  int
	TIME                     int
	TIMESTAMP                int
	DATETIME                 int
	YEAR                     int
	CHAR                     int
	VARCHAR                  int
	BOOL                     int
	CHARACTER                int
	VARBINARY                int
	NCHAR                    int
	TEXT                     int
	TINYTEXT                 int
	MEDIUMTEXT               int
	LONGTEXT                 int
	BLOB                     int
	TINYBLOB                 int
	MEDIUMBLOB               int
	LONGBLOB                 int
	JSON                     int
	ENUM                     int
	GEOMETRY                 int
	POINT                    int
	LINESTRING               int
	POLYGON                  int
	GEOMETRYCOLLECTION       int
	MULTIPOINT               int
	MULTILINESTRING          int
	MULTIPOLYGON             int
	CREATE                   int
	ALTER                    int
	DROP                     int
	RENAME                   int
	ANALYZE                  int
	ADD                      int
	SCHEMA                   int
	TABLE                    int
	INDEX                    int
	VIEW                     int
	TO                       int
	IGNORE                   int
	IF                       int
	UNIQUE                   int
	PRIMARY                  int
	COLUMN                   int
	CONSTRAINT               int
	SPATIAL                  int
	FULLTEXT                 int
	FULL                     int
	EXTENDED                 int
	FOREIGN                  int
	KEY_BLOCK_SIZE           int
	SHOW                     int
	DESCRIBE                 int
	EXPLAIN                  int
	DATE                     int
	ESCAPE                   int
	REPAIR                   int
	OPTIMIZE                 int
	TRUNCATE                 int
	MAXVALUE                 int
	PARTITION                int
	REORGANIZE               int
	LESS                     int
	THAN                     int
	PROCEDURE                int
	TRIGGER                  int
	STATUS                   int
	VARIABLES                int
	NULLX                    int
	AUTO_INCREMENT           int
	APPROXNUM                int
	SIGNED                   int
	UNSIGNED                 int
	ZEROFILL                 int
	DATABASES                int
	TABLES                   int
	PROCESSLIST              int
	NAMES                    int
	CHARSET                  int
	GLOBAL                   int
	SESSION                  int
	ISOLATION                int
	LEVEL                    int
	READ                     int
	WRITE                    int
	ONLY                     int
	REPEATABLE               int
	COMMITTED                int
	UNCOMMITTED              int
	SERIALIZABLE             int
	CURRENT_TIMESTAMP        int
	DATABASE                 int
	CURRENT_DATE             int
	CURRENT_TIME             int
	LOCALTIME                int
	LOCALTIMESTAMP           int
	UTC_DATE                 int
	UTC_TIME                 int
	UTC_TIMESTAMP            int
	REPLACE                  int
	CONVERT                  int
	CAST                     int
	SUBSTR                   int
	SUBSTRING                int
	GROUP_CONCAT             int
	SEPARATOR                int
	MATCH                    int
	AGAINST                  int
	BOOLEAN                  int
	LANGUAGE                 int
	WITH                     int
	QUERY                    int
	EXPANSION                int
	CURRENT_USER             int
	CURRENT_ROLE             int
	MONTH                    int
	CURTIME                  int
	SYSDATE                  int
	AVG                      int
	LOAD                     int
	INFILE                   int
	OUTFILE                  int
	HEADER                   int
	MAX_FILE_SIZE            int
	FORCE_QUOTE              int
	STARTING                 int
	LINES                    int
	UNUSED                   int
	INT1                     int
	INT2                     int
	INT3                     int
	INT4                     int
	INT8                     int
	VERBOSE                  int
)
