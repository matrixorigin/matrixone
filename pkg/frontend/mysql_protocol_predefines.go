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

package frontend

// tuple (collation name, charset)
type charsetCollationName struct {
	charset       string
	collationName string
}

// the map: collation id --> (charset, collation name)
// Run the SQL below in Mysql 8.0.23 to get the map.
// the SQL: select concat(RelationName,':\t\t{"',CHARACTER_SET_NAME,'",\t"',collation_name,'"},') from INFORMATION_SCHEMA.COLLATIONS order by id;
var collationID2CharsetAndName = map[int]charsetCollationName{
	1:   {"big5", "big5_chinese_ci"},
	2:   {"latin2", "latin2_czech_cs"},
	3:   {"dec8", "dec8_swedish_ci"},
	4:   {"cp850", "cp850_general_ci"},
	5:   {"latin1", "latin1_german1_ci"},
	6:   {"hp8", "hp8_english_ci"},
	7:   {"koi8r", "koi8r_general_ci"},
	8:   {"latin1", "latin1_swedish_ci"},
	9:   {"latin2", "latin2_general_ci"},
	10:  {"swe7", "swe7_swedish_ci"},
	11:  {"ascii", "ascii_general_ci"},
	12:  {"ujis", "ujis_japanese_ci"},
	13:  {"sjis", "sjis_japanese_ci"},
	14:  {"cp1251", "cp1251_bulgarian_ci"},
	15:  {"latin1", "latin1_danish_ci"},
	16:  {"hebrew", "hebrew_general_ci"},
	18:  {"tis620", "tis620_thai_ci"},
	19:  {"euckr", "euckr_korean_ci"},
	20:  {"latin7", "latin7_estonian_cs"},
	21:  {"latin2", "latin2_hungarian_ci"},
	22:  {"koi8u", "koi8u_general_ci"},
	23:  {"cp1251", "cp1251_ukrainian_ci"},
	24:  {"gb2312", "gb2312_chinese_ci"},
	25:  {"greek", "greek_general_ci"},
	26:  {"cp1250", "cp1250_general_ci"},
	27:  {"latin2", "latin2_croatian_ci"},
	28:  {"gbk", "gbk_chinese_ci"},
	29:  {"cp1257", "cp1257_lithuanian_ci"},
	30:  {"latin5", "latin5_turkish_ci"},
	31:  {"latin1", "latin1_german2_ci"},
	32:  {"armscii8", "armscii8_general_ci"},
	33:  {"utf8", "utf8_general_ci"},
	34:  {"cp1250", "cp1250_czech_cs"},
	35:  {"ucs2", "ucs2_general_ci"},
	36:  {"cp866", "cp866_general_ci"},
	37:  {"keybcs2", "keybcs2_general_ci"},
	38:  {"macce", "macce_general_ci"},
	39:  {"macroman", "macroman_general_ci"},
	40:  {"cp852", "cp852_general_ci"},
	41:  {"latin7", "latin7_general_ci"},
	42:  {"latin7", "latin7_general_cs"},
	43:  {"macce", "macce_bin"},
	44:  {"cp1250", "cp1250_croatian_ci"},
	45:  {"utf8mb4", "utf8mb4_general_ci"},
	46:  {"utf8mb4", "utf8mb4_bin"},
	47:  {"latin1", "latin1_bin"},
	48:  {"latin1", "latin1_general_ci"},
	49:  {"latin1", "latin1_general_cs"},
	50:  {"cp1251", "cp1251_bin"},
	51:  {"cp1251", "cp1251_general_ci"},
	52:  {"cp1251", "cp1251_general_cs"},
	53:  {"macroman", "macroman_bin"},
	54:  {"utf16", "utf16_general_ci"},
	55:  {"utf16", "utf16_bin"},
	56:  {"utf16le", "utf16le_general_ci"},
	57:  {"cp1256", "cp1256_general_ci"},
	58:  {"cp1257", "cp1257_bin"},
	59:  {"cp1257", "cp1257_general_ci"},
	60:  {"utf32", "utf32_general_ci"},
	61:  {"utf32", "utf32_bin"},
	62:  {"utf16le", "utf16le_bin"},
	63:  {"binary", "binary"},
	64:  {"armscii8", "armscii8_bin"},
	65:  {"ascii", "ascii_bin"},
	66:  {"cp1250", "cp1250_bin"},
	67:  {"cp1256", "cp1256_bin"},
	68:  {"cp866", "cp866_bin"},
	69:  {"dec8", "dec8_bin"},
	70:  {"greek", "greek_bin"},
	71:  {"hebrew", "hebrew_bin"},
	72:  {"hp8", "hp8_bin"},
	73:  {"keybcs2", "keybcs2_bin"},
	74:  {"koi8r", "koi8r_bin"},
	75:  {"koi8u", "koi8u_bin"},
	76:  {"utf8", "utf8_tolower_ci"},
	77:  {"latin2", "latin2_bin"},
	78:  {"latin5", "latin5_bin"},
	79:  {"latin7", "latin7_bin"},
	80:  {"cp850", "cp850_bin"},
	81:  {"cp852", "cp852_bin"},
	82:  {"swe7", "swe7_bin"},
	83:  {"utf8", "utf8_bin"},
	84:  {"big5", "big5_bin"},
	85:  {"euckr", "euckr_bin"},
	86:  {"gb2312", "gb2312_bin"},
	87:  {"gbk", "gbk_bin"},
	88:  {"sjis", "sjis_bin"},
	89:  {"tis620", "tis620_bin"},
	90:  {"ucs2", "ucs2_bin"},
	91:  {"ujis", "ujis_bin"},
	92:  {"geostd8", "geostd8_general_ci"},
	93:  {"geostd8", "geostd8_bin"},
	94:  {"latin1", "latin1_spanish_ci"},
	95:  {"cp932", "cp932_japanese_ci"},
	96:  {"cp932", "cp932_bin"},
	97:  {"eucjpms", "eucjpms_japanese_ci"},
	98:  {"eucjpms", "eucjpms_bin"},
	99:  {"cp1250", "cp1250_polish_ci"},
	101: {"utf16", "utf16_unicode_ci"},
	102: {"utf16", "utf16_icelandic_ci"},
	103: {"utf16", "utf16_latvian_ci"},
	104: {"utf16", "utf16_romanian_ci"},
	105: {"utf16", "utf16_slovenian_ci"},
	106: {"utf16", "utf16_polish_ci"},
	107: {"utf16", "utf16_estonian_ci"},
	108: {"utf16", "utf16_spanish_ci"},
	109: {"utf16", "utf16_swedish_ci"},
	110: {"utf16", "utf16_turkish_ci"},
	111: {"utf16", "utf16_czech_ci"},
	112: {"utf16", "utf16_danish_ci"},
	113: {"utf16", "utf16_lithuanian_ci"},
	114: {"utf16", "utf16_slovak_ci"},
	115: {"utf16", "utf16_spanish2_ci"},
	116: {"utf16", "utf16_roman_ci"},
	117: {"utf16", "utf16_persian_ci"},
	118: {"utf16", "utf16_esperanto_ci"},
	119: {"utf16", "utf16_hungarian_ci"},
	120: {"utf16", "utf16_sinhala_ci"},
	121: {"utf16", "utf16_german2_ci"},
	122: {"utf16", "utf16_croatian_ci"},
	123: {"utf16", "utf16_unicode_520_ci"},
	124: {"utf16", "utf16_vietnamese_ci"},
	128: {"ucs2", "ucs2_unicode_ci"},
	129: {"ucs2", "ucs2_icelandic_ci"},
	130: {"ucs2", "ucs2_latvian_ci"},
	131: {"ucs2", "ucs2_romanian_ci"},
	132: {"ucs2", "ucs2_slovenian_ci"},
	133: {"ucs2", "ucs2_polish_ci"},
	134: {"ucs2", "ucs2_estonian_ci"},
	135: {"ucs2", "ucs2_spanish_ci"},
	136: {"ucs2", "ucs2_swedish_ci"},
	137: {"ucs2", "ucs2_turkish_ci"},
	138: {"ucs2", "ucs2_czech_ci"},
	139: {"ucs2", "ucs2_danish_ci"},
	140: {"ucs2", "ucs2_lithuanian_ci"},
	141: {"ucs2", "ucs2_slovak_ci"},
	142: {"ucs2", "ucs2_spanish2_ci"},
	143: {"ucs2", "ucs2_roman_ci"},
	144: {"ucs2", "ucs2_persian_ci"},
	145: {"ucs2", "ucs2_esperanto_ci"},
	146: {"ucs2", "ucs2_hungarian_ci"},
	147: {"ucs2", "ucs2_sinhala_ci"},
	148: {"ucs2", "ucs2_german2_ci"},
	149: {"ucs2", "ucs2_croatian_ci"},
	150: {"ucs2", "ucs2_unicode_520_ci"},
	151: {"ucs2", "ucs2_vietnamese_ci"},
	159: {"ucs2", "ucs2_general_mysql500_ci"},
	160: {"utf32", "utf32_unicode_ci"},
	161: {"utf32", "utf32_icelandic_ci"},
	162: {"utf32", "utf32_latvian_ci"},
	163: {"utf32", "utf32_romanian_ci"},
	164: {"utf32", "utf32_slovenian_ci"},
	165: {"utf32", "utf32_polish_ci"},
	166: {"utf32", "utf32_estonian_ci"},
	167: {"utf32", "utf32_spanish_ci"},
	168: {"utf32", "utf32_swedish_ci"},
	169: {"utf32", "utf32_turkish_ci"},
	170: {"utf32", "utf32_czech_ci"},
	171: {"utf32", "utf32_danish_ci"},
	172: {"utf32", "utf32_lithuanian_ci"},
	173: {"utf32", "utf32_slovak_ci"},
	174: {"utf32", "utf32_spanish2_ci"},
	175: {"utf32", "utf32_roman_ci"},
	176: {"utf32", "utf32_persian_ci"},
	177: {"utf32", "utf32_esperanto_ci"},
	178: {"utf32", "utf32_hungarian_ci"},
	179: {"utf32", "utf32_sinhala_ci"},
	180: {"utf32", "utf32_german2_ci"},
	181: {"utf32", "utf32_croatian_ci"},
	182: {"utf32", "utf32_unicode_520_ci"},
	183: {"utf32", "utf32_vietnamese_ci"},
	192: {"utf8", "utf8_unicode_ci"},
	193: {"utf8", "utf8_icelandic_ci"},
	194: {"utf8", "utf8_latvian_ci"},
	195: {"utf8", "utf8_romanian_ci"},
	196: {"utf8", "utf8_slovenian_ci"},
	197: {"utf8", "utf8_polish_ci"},
	198: {"utf8", "utf8_estonian_ci"},
	199: {"utf8", "utf8_spanish_ci"},
	200: {"utf8", "utf8_swedish_ci"},
	201: {"utf8", "utf8_turkish_ci"},
	202: {"utf8", "utf8_czech_ci"},
	203: {"utf8", "utf8_danish_ci"},
	204: {"utf8", "utf8_lithuanian_ci"},
	205: {"utf8", "utf8_slovak_ci"},
	206: {"utf8", "utf8_spanish2_ci"},
	207: {"utf8", "utf8_roman_ci"},
	208: {"utf8", "utf8_persian_ci"},
	209: {"utf8", "utf8_esperanto_ci"},
	210: {"utf8", "utf8_hungarian_ci"},
	211: {"utf8", "utf8_sinhala_ci"},
	212: {"utf8", "utf8_german2_ci"},
	213: {"utf8", "utf8_croatian_ci"},
	214: {"utf8", "utf8_unicode_520_ci"},
	215: {"utf8", "utf8_vietnamese_ci"},
	223: {"utf8", "utf8_general_mysql500_ci"},
	224: {"utf8mb4", "utf8mb4_unicode_ci"},
	225: {"utf8mb4", "utf8mb4_icelandic_ci"},
	226: {"utf8mb4", "utf8mb4_latvian_ci"},
	227: {"utf8mb4", "utf8mb4_romanian_ci"},
	228: {"utf8mb4", "utf8mb4_slovenian_ci"},
	229: {"utf8mb4", "utf8mb4_polish_ci"},
	230: {"utf8mb4", "utf8mb4_estonian_ci"},
	231: {"utf8mb4", "utf8mb4_spanish_ci"},
	232: {"utf8mb4", "utf8mb4_swedish_ci"},
	233: {"utf8mb4", "utf8mb4_turkish_ci"},
	234: {"utf8mb4", "utf8mb4_czech_ci"},
	235: {"utf8mb4", "utf8mb4_danish_ci"},
	236: {"utf8mb4", "utf8mb4_lithuanian_ci"},
	237: {"utf8mb4", "utf8mb4_slovak_ci"},
	238: {"utf8mb4", "utf8mb4_spanish2_ci"},
	239: {"utf8mb4", "utf8mb4_roman_ci"},
	240: {"utf8mb4", "utf8mb4_persian_ci"},
	241: {"utf8mb4", "utf8mb4_esperanto_ci"},
	242: {"utf8mb4", "utf8mb4_hungarian_ci"},
	243: {"utf8mb4", "utf8mb4_sinhala_ci"},
	244: {"utf8mb4", "utf8mb4_german2_ci"},
	245: {"utf8mb4", "utf8mb4_croatian_ci"},
	246: {"utf8mb4", "utf8mb4_unicode_520_ci"},
	247: {"utf8mb4", "utf8mb4_vietnamese_ci"},
	248: {"gb18030", "gb18030_chinese_ci"},
	249: {"gb18030", "gb18030_bin"},
	250: {"gb18030", "gb18030_unicode_520_ci"},
	255: {"utf8mb4", "utf8mb4_0900_ai_ci"},
	256: {"utf8mb4", "utf8mb4_de_pb_0900_ai_ci"},
	257: {"utf8mb4", "utf8mb4_is_0900_ai_ci"},
	258: {"utf8mb4", "utf8mb4_lv_0900_ai_ci"},
	259: {"utf8mb4", "utf8mb4_ro_0900_ai_ci"},
	260: {"utf8mb4", "utf8mb4_sl_0900_ai_ci"},
	261: {"utf8mb4", "utf8mb4_pl_0900_ai_ci"},
	262: {"utf8mb4", "utf8mb4_et_0900_ai_ci"},
	263: {"utf8mb4", "utf8mb4_es_0900_ai_ci"},
	264: {"utf8mb4", "utf8mb4_sv_0900_ai_ci"},
	265: {"utf8mb4", "utf8mb4_tr_0900_ai_ci"},
	266: {"utf8mb4", "utf8mb4_cs_0900_ai_ci"},
	267: {"utf8mb4", "utf8mb4_da_0900_ai_ci"},
	268: {"utf8mb4", "utf8mb4_lt_0900_ai_ci"},
	269: {"utf8mb4", "utf8mb4_sk_0900_ai_ci"},
	270: {"utf8mb4", "utf8mb4_es_trad_0900_ai_ci"},
	271: {"utf8mb4", "utf8mb4_la_0900_ai_ci"},
	273: {"utf8mb4", "utf8mb4_eo_0900_ai_ci"},
	274: {"utf8mb4", "utf8mb4_hu_0900_ai_ci"},
	275: {"utf8mb4", "utf8mb4_hr_0900_ai_ci"},
	277: {"utf8mb4", "utf8mb4_vi_0900_ai_ci"},
	278: {"utf8mb4", "utf8mb4_0900_as_cs"},
	279: {"utf8mb4", "utf8mb4_de_pb_0900_as_cs"},
	280: {"utf8mb4", "utf8mb4_is_0900_as_cs"},
	281: {"utf8mb4", "utf8mb4_lv_0900_as_cs"},
	282: {"utf8mb4", "utf8mb4_ro_0900_as_cs"},
	283: {"utf8mb4", "utf8mb4_sl_0900_as_cs"},
	284: {"utf8mb4", "utf8mb4_pl_0900_as_cs"},
	285: {"utf8mb4", "utf8mb4_et_0900_as_cs"},
	286: {"utf8mb4", "utf8mb4_es_0900_as_cs"},
	287: {"utf8mb4", "utf8mb4_sv_0900_as_cs"},
	288: {"utf8mb4", "utf8mb4_tr_0900_as_cs"},
	289: {"utf8mb4", "utf8mb4_cs_0900_as_cs"},
	290: {"utf8mb4", "utf8mb4_da_0900_as_cs"},
	291: {"utf8mb4", "utf8mb4_lt_0900_as_cs"},
	292: {"utf8mb4", "utf8mb4_sk_0900_as_cs"},
	293: {"utf8mb4", "utf8mb4_es_trad_0900_as_cs"},
	294: {"utf8mb4", "utf8mb4_la_0900_as_cs"},
	296: {"utf8mb4", "utf8mb4_eo_0900_as_cs"},
	297: {"utf8mb4", "utf8mb4_hu_0900_as_cs"},
	298: {"utf8mb4", "utf8mb4_hr_0900_as_cs"},
	300: {"utf8mb4", "utf8mb4_vi_0900_as_cs"},
	303: {"utf8mb4", "utf8mb4_ja_0900_as_cs"},
	304: {"utf8mb4", "utf8mb4_ja_0900_as_cs_ks"},
	305: {"utf8mb4", "utf8mb4_0900_as_ci"},
	306: {"utf8mb4", "utf8mb4_ru_0900_ai_ci"},
	307: {"utf8mb4", "utf8mb4_ru_0900_as_cs"},
	308: {"utf8mb4", "utf8mb4_zh_0900_as_cs"},
	309: {"utf8mb4", "utf8mb4_0900_bin"},
}

// mysql client capabilities
const (
	CLIENT_LONG_PASSWORD                  uint32 = 0x00000001
	CLIENT_FOUND_ROWS                     uint32 = 0x00000002
	CLIENT_LONG_FLAG                      uint32 = 0x00000004
	CLIENT_CONNECT_WITH_DB                uint32 = 0x00000008
	CLIENT_NO_SCHEMA                      uint32 = 0x00000010
	CLIENT_COMPRESS                       uint32 = 0x00000020
	CLIENT_LOCAL_FILES                    uint32 = 0x00000080
	CLIENT_IGNORE_SPACE                   uint32 = 0x00000100
	CLIENT_PROTOCOL_41                    uint32 = 0x00000200
	CLIENT_INTERACTIVE                    uint32 = 0x00000400
	CLIENT_SSL                            uint32 = 0x00000800
	CLIENT_IGNORE_SIGPIPE                 uint32 = 0x00001000
	CLIENT_TRANSACTIONS                   uint32 = 0x00002000
	CLIENT_RESERVED                       uint32 = 0x00004000
	CLIENT_SECURE_CONNECTION              uint32 = 0x00008000
	CLIENT_MULTI_STATEMENTS               uint32 = 0x00010000
	CLIENT_MULTI_RESULTS                  uint32 = 0x00020000
	CLIENT_PS_MULTI_RESULTS               uint32 = 0x00040000
	CLIENT_PLUGIN_AUTH                    uint32 = 0x00080000
	CLIENT_CONNECT_ATTRS                  uint32 = 0x00100000
	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA uint32 = 0x00200000
	CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS   uint32 = 0x00400000
	CLIENT_SESSION_TRACK                  uint32 = 0x00800000
	CLIENT_DEPRECATE_EOF                  uint32 = 0x01000000
)

// server status
const (
	SERVER_STATUS_IN_TRANS             uint16 = 0x0001
	SERVER_STATUS_AUTOCOMMIT           uint16 = 0x0002
	SERVER_MORE_RESULTS_EXISTS         uint16 = 0x0008
	SERVER_STATUS_NO_GOOD_INDEX_USED   uint16 = 0x0010
	SERVER_STATUS_NO_INDEX_USED        uint16 = 0x0020
	SERVER_STATUS_CURSOR_EXISTS        uint16 = 0x0040
	SERVER_STATUS_LAST_ROW_SENT        uint16 = 0x0080
	SERVER_STATUS_DB_DROPPED           uint16 = 0x0100
	SERVER_STATUS_NO_BACKSLASH_ESCAPES uint16 = 0x0200
	SERVER_STATUS_METADATA_CHANGED     uint16 = 0x0400
	SERVER_QUERY_WAS_SLOW              uint16 = 0x0800
	SERVER_PS_OUT_PARAMS               uint16 = 0x1000
	SERVER_STATUS_IN_TRANS_READONLY    uint16 = 0x2000
	SERVER_SESSION_STATE_CHANGED       uint16 = 0x4000
)

type CommandType uint8

// text protocol in mysql client protocol
// iteration command
const (
	COM_SLEEP               CommandType = 0x00
	COM_QUIT                CommandType = 0x01
	COM_INIT_DB             CommandType = 0x02
	COM_QUERY               CommandType = 0x03
	COM_FIELD_LIST          CommandType = 0x04
	COM_CREATE_DB           CommandType = 0x05
	COM_DROP_DB             CommandType = 0x06
	COM_REFRESH             CommandType = 0x07
	COM_SHUTDOWN            CommandType = 0x08
	COM_STATISTICS          CommandType = 0x09
	COM_PROCESS_INFO        CommandType = 0x0a
	COM_CONNECT             CommandType = 0x0b
	COM_PROCESS_KILL        CommandType = 0x0c
	COM_DEBUG               CommandType = 0x0d
	COM_PING                CommandType = 0x0e
	COM_TIME                CommandType = 0x0f
	COM_DELAYED_INSERT      CommandType = 0x10
	COM_CHANGE_USER         CommandType = 0x11
	COM_STMT_PREPARE        CommandType = 0x16
	COM_STMT_EXECUTE        CommandType = 0x17
	COM_STMT_SEND_LONG_DATA CommandType = 0x18
	COM_STMT_CLOSE          CommandType = 0x19
	COM_STMT_RESET          CommandType = 0x1a
	COM_SET_OPTION          CommandType = 0x1b
	COM_STMT_FETCH          CommandType = 0x1c
	COM_DAEMON              CommandType = 0x1d
	COM_RESET_CONNECTION    CommandType = 0x1f
)

func (ct CommandType) String() string {
	switch ct {
	case COM_SLEEP:
		return "COM_SLEEP"
	case COM_QUIT:
		return "COM_QUIT"
	case COM_INIT_DB:
		return "COM_INIT_DB"
	case COM_QUERY:
		return "COM_QUERY"
	case COM_FIELD_LIST:
		return "COM_FIELD_LIST"
	case COM_CREATE_DB:
		return "COM_CREATE_DB"
	case COM_DROP_DB:
		return "COM_DROP_DB"
	case COM_REFRESH:
		return "COM_REFRESH"
	case COM_SHUTDOWN:
		return "COM_SHUTDOWN"
	case COM_STATISTICS:
		return "COM_STATISTICS"
	case COM_PROCESS_INFO:
		return "COM_PROCESS_INFO"
	case COM_CONNECT:
		return "COM_CONNECT"
	case COM_PROCESS_KILL:
		return "COM_PROCESS_KILL"
	case COM_DEBUG:
		return "COM_DEBUG"
	case COM_PING:
		return "COM_PING"
	case COM_TIME:
		return "COM_TIME"
	case COM_DELAYED_INSERT:
		return "COM_DELAYED_INSERT"
	case COM_CHANGE_USER:
		return "COM_CHANGE_USER"
	case COM_STMT_PREPARE:
		return "COM_STMT_PREPARE"
	case COM_STMT_EXECUTE:
		return "COM_STMT_EXECUTE"
	case COM_STMT_SEND_LONG_DATA:
		return "COM_STMT_SEND_LONG_DATA"
	case COM_STMT_CLOSE:
		return "COM_STMT_CLOSE"
	case COM_STMT_RESET:
		return "COM_STMT_RESET"
	case COM_SET_OPTION:
		return "COM_SET_OPTION"
	case COM_STMT_FETCH:
		return "COM_STMT_FETCH"
	case COM_DAEMON:
		return "COM_DAEMON"
	case COM_RESET_CONNECTION:
		return "COM_RESET_CONNECTION"
	default:
		return ""
	}
}

// reference to sql/query_options.h in mysql server 8.0.23
const (
	OPTION_AUTOCOMMIT                     uint32 = 1 << 8
	OPTION_BIG_SELECTS                    uint32 = 1 << 9
	OPTION_LOG_OFF                        uint32 = 1 << 10
	OPTION_QUOTE_SHOW_CREATE              uint32 = 1 << 11
	TMP_TABLE_ALL_COLUMNS                 uint32 = 1 << 12
	OPTION_WARNINGS                       uint32 = 1 << 13
	OPTION_AUTO_IS_NULL                   uint32 = 1 << 14
	OPTION_FOUND_COMMENT                  uint32 = 1 << 15
	OPTION_SAFE_UPDATES                   uint32 = 1 << 16
	OPTION_BUFFER_RESULT                  uint32 = 1 << 17
	OPTION_BIN_LOG                        uint32 = 1 << 18
	OPTION_NOT_AUTOCOMMIT                 uint32 = 1 << 19
	OPTION_BEGIN                          uint32 = 1 << 20
	OPTION_TABLE_LOCK                     uint32 = 1 << 21
	OPTION_QUICK                          uint32 = 1 << 22
	OPTION_NO_CONST_TABLES                uint32 = 1 << 23
	OPTION_ATTACH_ABORT_TRANSACTION_ERROR uint32 = 1 << 24 //defined in mo
)
