package server

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"math/rand"
	"time"
)

//tuple (collation id, collation name)
type collationIDName struct {
	collationID int
	collationName string
}

//the map: charset --> default (collation id, collation name)
//the relation between the charset with the collation: one charset can correspond to multiple collations.
//so there is a default collation for the charset.
//Run the SQL below in Mysql 8.0.23 to get the map.
//the SQL:select concat('"',A.character_set_name,'":\t\t{',B.ID,',\t"',A.default_collate_name,'"},') from INFORMATION_SCHEMA.CHARACTER_SETS AS A join INFORMATION_SCHEMA.COLLATIONS AS B on (A.default_collate_name = B.collation_name) order by A.character_set_name ;
var charset2Collation=map[string]collationIDName{
	"armscii8":		{32,	"armscii8_general_ci"},
	"ascii":		{11,	"ascii_general_ci"},
	"big5":		{1,	"big5_chinese_ci"},
	"binary":		{63,	"binary"},
	"cp1250":		{26,	"cp1250_general_ci"},
	"cp1251":		{51,	"cp1251_general_ci"},
	"cp1256":		{57,	"cp1256_general_ci"},
	"cp1257":		{59,	"cp1257_general_ci"},
	"cp850":		{4,	"cp850_general_ci"},
	"cp852":		{40,	"cp852_general_ci"},
	"cp866":		{36,	"cp866_general_ci"},
	"cp932":		{95,	"cp932_japanese_ci"},
	"dec8":		{3,	"dec8_swedish_ci"},
	"eucjpms":		{97,	"eucjpms_japanese_ci"},
	"euckr":		{19,	"euckr_korean_ci"},
	"gb18030":		{248,	"gb18030_chinese_ci"},
	"gb2312":		{24,	"gb2312_chinese_ci"},
	"gbk":		{28,	"gbk_chinese_ci"},
	"geostd8":		{92,	"geostd8_general_ci"},
	"greek":		{25,	"greek_general_ci"},
	"hebrew":		{16,	"hebrew_general_ci"},
	"hp8":		{6,	"hp8_english_ci"},
	"keybcs2":		{37,	"keybcs2_general_ci"},
	"koi8r":		{7,	"koi8r_general_ci"},
	"koi8u":		{22,	"koi8u_general_ci"},
	"latin1":		{8,	"latin1_swedish_ci"},
	"latin2":		{9,	"latin2_general_ci"},
	"latin5":		{30,	"latin5_turkish_ci"},
	"latin7":		{41,	"latin7_general_ci"},
	"macce":		{38,	"macce_general_ci"},
	"macroman":		{39,	"macroman_general_ci"},
	"sjis":		{13,	"sjis_japanese_ci"},
	"swe7":		{10,	"swe7_swedish_ci"},
	"tis620":		{18,	"tis620_thai_ci"},
	"ucs2":		{35,	"ucs2_general_ci"},
	"ujis":		{12,	"ujis_japanese_ci"},
	"utf16":		{54,	"utf16_general_ci"},
	"utf16le":		{56,	"utf16le_general_ci"},
	"utf32":		{60,	"utf32_general_ci"},
	"utf8":		{33,	"utf8_general_ci"},
	"utf8mb4":		{255,	"utf8mb4_0900_ai_ci"},
}

//tuple (collation name, charset)
type charsetCollationName struct {
	charset string
	collationName string
}

//the map: collation id --> (charset, collation name)
//Run the SQL below in Mysql 8.0.23 to get the map.
//the SQL: select concat(ID,':\t\t{"',CHARACTER_SET_NAME,'",\t"',collation_name,'"},') from INFORMATION_SCHEMA.COLLATIONS order by id;
var collationID2CharsetAndName = map[int]charsetCollationName{
	1:		{"big5",	"big5_chinese_ci"},
	2:		{"latin2",	"latin2_czech_cs"},
	3:		{"dec8",	"dec8_swedish_ci"},
	4:		{"cp850",	"cp850_general_ci"},
	5:		{"latin1",	"latin1_german1_ci"},
	6:		{"hp8",	"hp8_english_ci"},
	7:		{"koi8r",	"koi8r_general_ci"},
	8:		{"latin1",	"latin1_swedish_ci"},
	9:		{"latin2",	"latin2_general_ci"},
	10:		{"swe7",	"swe7_swedish_ci"},
	11:		{"ascii",	"ascii_general_ci"},
	12:		{"ujis",	"ujis_japanese_ci"},
	13:		{"sjis",	"sjis_japanese_ci"},
	14:		{"cp1251",	"cp1251_bulgarian_ci"},
	15:		{"latin1",	"latin1_danish_ci"},
	16:		{"hebrew",	"hebrew_general_ci"},
	18:		{"tis620",	"tis620_thai_ci"},
	19:		{"euckr",	"euckr_korean_ci"},
	20:		{"latin7",	"latin7_estonian_cs"},
	21:		{"latin2",	"latin2_hungarian_ci"},
	22:		{"koi8u",	"koi8u_general_ci"},
	23:		{"cp1251",	"cp1251_ukrainian_ci"},
	24:		{"gb2312",	"gb2312_chinese_ci"},
	25:		{"greek",	"greek_general_ci"},
	26:		{"cp1250",	"cp1250_general_ci"},
	27:		{"latin2",	"latin2_croatian_ci"},
	28:		{"gbk",	"gbk_chinese_ci"},
	29:		{"cp1257",	"cp1257_lithuanian_ci"},
	30:		{"latin5",	"latin5_turkish_ci"},
	31:		{"latin1",	"latin1_german2_ci"},
	32:		{"armscii8",	"armscii8_general_ci"},
	33:		{"utf8",	"utf8_general_ci"},
	34:		{"cp1250",	"cp1250_czech_cs"},
	35:		{"ucs2",	"ucs2_general_ci"},
	36:		{"cp866",	"cp866_general_ci"},
	37:		{"keybcs2",	"keybcs2_general_ci"},
	38:		{"macce",	"macce_general_ci"},
	39:		{"macroman",	"macroman_general_ci"},
	40:		{"cp852",	"cp852_general_ci"},
	41:		{"latin7",	"latin7_general_ci"},
	42:		{"latin7",	"latin7_general_cs"},
	43:		{"macce",	"macce_bin"},
	44:		{"cp1250",	"cp1250_croatian_ci"},
	45:		{"utf8mb4",	"utf8mb4_general_ci"},
	46:		{"utf8mb4",	"utf8mb4_bin"},
	47:		{"latin1",	"latin1_bin"},
	48:		{"latin1",	"latin1_general_ci"},
	49:		{"latin1",	"latin1_general_cs"},
	50:		{"cp1251",	"cp1251_bin"},
	51:		{"cp1251",	"cp1251_general_ci"},
	52:		{"cp1251",	"cp1251_general_cs"},
	53:		{"macroman",	"macroman_bin"},
	54:		{"utf16",	"utf16_general_ci"},
	55:		{"utf16",	"utf16_bin"},
	56:		{"utf16le",	"utf16le_general_ci"},
	57:		{"cp1256",	"cp1256_general_ci"},
	58:		{"cp1257",	"cp1257_bin"},
	59:		{"cp1257",	"cp1257_general_ci"},
	60:		{"utf32",	"utf32_general_ci"},
	61:		{"utf32",	"utf32_bin"},
	62:		{"utf16le",	"utf16le_bin"},
	63:		{"binary",	"binary"},
	64:		{"armscii8",	"armscii8_bin"},
	65:		{"ascii",	"ascii_bin"},
	66:		{"cp1250",	"cp1250_bin"},
	67:		{"cp1256",	"cp1256_bin"},
	68:		{"cp866",	"cp866_bin"},
	69:		{"dec8",	"dec8_bin"},
	70:		{"greek",	"greek_bin"},
	71:		{"hebrew",	"hebrew_bin"},
	72:		{"hp8",	"hp8_bin"},
	73:		{"keybcs2",	"keybcs2_bin"},
	74:		{"koi8r",	"koi8r_bin"},
	75:		{"koi8u",	"koi8u_bin"},
	76:		{"utf8",	"utf8_tolower_ci"},
	77:		{"latin2",	"latin2_bin"},
	78:		{"latin5",	"latin5_bin"},
	79:		{"latin7",	"latin7_bin"},
	80:		{"cp850",	"cp850_bin"},
	81:		{"cp852",	"cp852_bin"},
	82:		{"swe7",	"swe7_bin"},
	83:		{"utf8",	"utf8_bin"},
	84:		{"big5",	"big5_bin"},
	85:		{"euckr",	"euckr_bin"},
	86:		{"gb2312",	"gb2312_bin"},
	87:		{"gbk",	"gbk_bin"},
	88:		{"sjis",	"sjis_bin"},
	89:		{"tis620",	"tis620_bin"},
	90:		{"ucs2",	"ucs2_bin"},
	91:		{"ujis",	"ujis_bin"},
	92:		{"geostd8",	"geostd8_general_ci"},
	93:		{"geostd8",	"geostd8_bin"},
	94:		{"latin1",	"latin1_spanish_ci"},
	95:		{"cp932",	"cp932_japanese_ci"},
	96:		{"cp932",	"cp932_bin"},
	97:		{"eucjpms",	"eucjpms_japanese_ci"},
	98:		{"eucjpms",	"eucjpms_bin"},
	99:		{"cp1250",	"cp1250_polish_ci"},
	101:		{"utf16",	"utf16_unicode_ci"},
	102:		{"utf16",	"utf16_icelandic_ci"},
	103:		{"utf16",	"utf16_latvian_ci"},
	104:		{"utf16",	"utf16_romanian_ci"},
	105:		{"utf16",	"utf16_slovenian_ci"},
	106:		{"utf16",	"utf16_polish_ci"},
	107:		{"utf16",	"utf16_estonian_ci"},
	108:		{"utf16",	"utf16_spanish_ci"},
	109:		{"utf16",	"utf16_swedish_ci"},
	110:		{"utf16",	"utf16_turkish_ci"},
	111:		{"utf16",	"utf16_czech_ci"},
	112:		{"utf16",	"utf16_danish_ci"},
	113:		{"utf16",	"utf16_lithuanian_ci"},
	114:		{"utf16",	"utf16_slovak_ci"},
	115:		{"utf16",	"utf16_spanish2_ci"},
	116:		{"utf16",	"utf16_roman_ci"},
	117:		{"utf16",	"utf16_persian_ci"},
	118:		{"utf16",	"utf16_esperanto_ci"},
	119:		{"utf16",	"utf16_hungarian_ci"},
	120:		{"utf16",	"utf16_sinhala_ci"},
	121:		{"utf16",	"utf16_german2_ci"},
	122:		{"utf16",	"utf16_croatian_ci"},
	123:		{"utf16",	"utf16_unicode_520_ci"},
	124:		{"utf16",	"utf16_vietnamese_ci"},
	128:		{"ucs2",	"ucs2_unicode_ci"},
	129:		{"ucs2",	"ucs2_icelandic_ci"},
	130:		{"ucs2",	"ucs2_latvian_ci"},
	131:		{"ucs2",	"ucs2_romanian_ci"},
	132:		{"ucs2",	"ucs2_slovenian_ci"},
	133:		{"ucs2",	"ucs2_polish_ci"},
	134:		{"ucs2",	"ucs2_estonian_ci"},
	135:		{"ucs2",	"ucs2_spanish_ci"},
	136:		{"ucs2",	"ucs2_swedish_ci"},
	137:		{"ucs2",	"ucs2_turkish_ci"},
	138:		{"ucs2",	"ucs2_czech_ci"},
	139:		{"ucs2",	"ucs2_danish_ci"},
	140:		{"ucs2",	"ucs2_lithuanian_ci"},
	141:		{"ucs2",	"ucs2_slovak_ci"},
	142:		{"ucs2",	"ucs2_spanish2_ci"},
	143:		{"ucs2",	"ucs2_roman_ci"},
	144:		{"ucs2",	"ucs2_persian_ci"},
	145:		{"ucs2",	"ucs2_esperanto_ci"},
	146:		{"ucs2",	"ucs2_hungarian_ci"},
	147:		{"ucs2",	"ucs2_sinhala_ci"},
	148:		{"ucs2",	"ucs2_german2_ci"},
	149:		{"ucs2",	"ucs2_croatian_ci"},
	150:		{"ucs2",	"ucs2_unicode_520_ci"},
	151:		{"ucs2",	"ucs2_vietnamese_ci"},
	159:		{"ucs2",	"ucs2_general_mysql500_ci"},
	160:		{"utf32",	"utf32_unicode_ci"},
	161:		{"utf32",	"utf32_icelandic_ci"},
	162:		{"utf32",	"utf32_latvian_ci"},
	163:		{"utf32",	"utf32_romanian_ci"},
	164:		{"utf32",	"utf32_slovenian_ci"},
	165:		{"utf32",	"utf32_polish_ci"},
	166:		{"utf32",	"utf32_estonian_ci"},
	167:		{"utf32",	"utf32_spanish_ci"},
	168:		{"utf32",	"utf32_swedish_ci"},
	169:		{"utf32",	"utf32_turkish_ci"},
	170:		{"utf32",	"utf32_czech_ci"},
	171:		{"utf32",	"utf32_danish_ci"},
	172:		{"utf32",	"utf32_lithuanian_ci"},
	173:		{"utf32",	"utf32_slovak_ci"},
	174:		{"utf32",	"utf32_spanish2_ci"},
	175:		{"utf32",	"utf32_roman_ci"},
	176:		{"utf32",	"utf32_persian_ci"},
	177:		{"utf32",	"utf32_esperanto_ci"},
	178:		{"utf32",	"utf32_hungarian_ci"},
	179:		{"utf32",	"utf32_sinhala_ci"},
	180:		{"utf32",	"utf32_german2_ci"},
	181:		{"utf32",	"utf32_croatian_ci"},
	182:		{"utf32",	"utf32_unicode_520_ci"},
	183:		{"utf32",	"utf32_vietnamese_ci"},
	192:		{"utf8",	"utf8_unicode_ci"},
	193:		{"utf8",	"utf8_icelandic_ci"},
	194:		{"utf8",	"utf8_latvian_ci"},
	195:		{"utf8",	"utf8_romanian_ci"},
	196:		{"utf8",	"utf8_slovenian_ci"},
	197:		{"utf8",	"utf8_polish_ci"},
	198:		{"utf8",	"utf8_estonian_ci"},
	199:		{"utf8",	"utf8_spanish_ci"},
	200:		{"utf8",	"utf8_swedish_ci"},
	201:		{"utf8",	"utf8_turkish_ci"},
	202:		{"utf8",	"utf8_czech_ci"},
	203:		{"utf8",	"utf8_danish_ci"},
	204:		{"utf8",	"utf8_lithuanian_ci"},
	205:		{"utf8",	"utf8_slovak_ci"},
	206:		{"utf8",	"utf8_spanish2_ci"},
	207:		{"utf8",	"utf8_roman_ci"},
	208:		{"utf8",	"utf8_persian_ci"},
	209:		{"utf8",	"utf8_esperanto_ci"},
	210:		{"utf8",	"utf8_hungarian_ci"},
	211:		{"utf8",	"utf8_sinhala_ci"},
	212:		{"utf8",	"utf8_german2_ci"},
	213:		{"utf8",	"utf8_croatian_ci"},
	214:		{"utf8",	"utf8_unicode_520_ci"},
	215:		{"utf8",	"utf8_vietnamese_ci"},
	223:		{"utf8",	"utf8_general_mysql500_ci"},
	224:		{"utf8mb4",	"utf8mb4_unicode_ci"},
	225:		{"utf8mb4",	"utf8mb4_icelandic_ci"},
	226:		{"utf8mb4",	"utf8mb4_latvian_ci"},
	227:		{"utf8mb4",	"utf8mb4_romanian_ci"},
	228:		{"utf8mb4",	"utf8mb4_slovenian_ci"},
	229:		{"utf8mb4",	"utf8mb4_polish_ci"},
	230:		{"utf8mb4",	"utf8mb4_estonian_ci"},
	231:		{"utf8mb4",	"utf8mb4_spanish_ci"},
	232:		{"utf8mb4",	"utf8mb4_swedish_ci"},
	233:		{"utf8mb4",	"utf8mb4_turkish_ci"},
	234:		{"utf8mb4",	"utf8mb4_czech_ci"},
	235:		{"utf8mb4",	"utf8mb4_danish_ci"},
	236:		{"utf8mb4",	"utf8mb4_lithuanian_ci"},
	237:		{"utf8mb4",	"utf8mb4_slovak_ci"},
	238:		{"utf8mb4",	"utf8mb4_spanish2_ci"},
	239:		{"utf8mb4",	"utf8mb4_roman_ci"},
	240:		{"utf8mb4",	"utf8mb4_persian_ci"},
	241:		{"utf8mb4",	"utf8mb4_esperanto_ci"},
	242:		{"utf8mb4",	"utf8mb4_hungarian_ci"},
	243:		{"utf8mb4",	"utf8mb4_sinhala_ci"},
	244:		{"utf8mb4",	"utf8mb4_german2_ci"},
	245:		{"utf8mb4",	"utf8mb4_croatian_ci"},
	246:		{"utf8mb4",	"utf8mb4_unicode_520_ci"},
	247:		{"utf8mb4",	"utf8mb4_vietnamese_ci"},
	248:		{"gb18030",	"gb18030_chinese_ci"},
	249:		{"gb18030",	"gb18030_bin"},
	250:		{"gb18030",	"gb18030_unicode_520_ci"},
	255:		{"utf8mb4",	"utf8mb4_0900_ai_ci"},
	256:		{"utf8mb4",	"utf8mb4_de_pb_0900_ai_ci"},
	257:		{"utf8mb4",	"utf8mb4_is_0900_ai_ci"},
	258:		{"utf8mb4",	"utf8mb4_lv_0900_ai_ci"},
	259:		{"utf8mb4",	"utf8mb4_ro_0900_ai_ci"},
	260:		{"utf8mb4",	"utf8mb4_sl_0900_ai_ci"},
	261:		{"utf8mb4",	"utf8mb4_pl_0900_ai_ci"},
	262:		{"utf8mb4",	"utf8mb4_et_0900_ai_ci"},
	263:		{"utf8mb4",	"utf8mb4_es_0900_ai_ci"},
	264:		{"utf8mb4",	"utf8mb4_sv_0900_ai_ci"},
	265:		{"utf8mb4",	"utf8mb4_tr_0900_ai_ci"},
	266:		{"utf8mb4",	"utf8mb4_cs_0900_ai_ci"},
	267:		{"utf8mb4",	"utf8mb4_da_0900_ai_ci"},
	268:		{"utf8mb4",	"utf8mb4_lt_0900_ai_ci"},
	269:		{"utf8mb4",	"utf8mb4_sk_0900_ai_ci"},
	270:		{"utf8mb4",	"utf8mb4_es_trad_0900_ai_ci"},
	271:		{"utf8mb4",	"utf8mb4_la_0900_ai_ci"},
	273:		{"utf8mb4",	"utf8mb4_eo_0900_ai_ci"},
	274:		{"utf8mb4",	"utf8mb4_hu_0900_ai_ci"},
	275:		{"utf8mb4",	"utf8mb4_hr_0900_ai_ci"},
	277:		{"utf8mb4",	"utf8mb4_vi_0900_ai_ci"},
	278:		{"utf8mb4",	"utf8mb4_0900_as_cs"},
	279:		{"utf8mb4",	"utf8mb4_de_pb_0900_as_cs"},
	280:		{"utf8mb4",	"utf8mb4_is_0900_as_cs"},
	281:		{"utf8mb4",	"utf8mb4_lv_0900_as_cs"},
	282:		{"utf8mb4",	"utf8mb4_ro_0900_as_cs"},
	283:		{"utf8mb4",	"utf8mb4_sl_0900_as_cs"},
	284:		{"utf8mb4",	"utf8mb4_pl_0900_as_cs"},
	285:		{"utf8mb4",	"utf8mb4_et_0900_as_cs"},
	286:		{"utf8mb4",	"utf8mb4_es_0900_as_cs"},
	287:		{"utf8mb4",	"utf8mb4_sv_0900_as_cs"},
	288:		{"utf8mb4",	"utf8mb4_tr_0900_as_cs"},
	289:		{"utf8mb4",	"utf8mb4_cs_0900_as_cs"},
	290:		{"utf8mb4",	"utf8mb4_da_0900_as_cs"},
	291:		{"utf8mb4",	"utf8mb4_lt_0900_as_cs"},
	292:		{"utf8mb4",	"utf8mb4_sk_0900_as_cs"},
	293:		{"utf8mb4",	"utf8mb4_es_trad_0900_as_cs"},
	294:		{"utf8mb4",	"utf8mb4_la_0900_as_cs"},
	296:		{"utf8mb4",	"utf8mb4_eo_0900_as_cs"},
	297:		{"utf8mb4",	"utf8mb4_hu_0900_as_cs"},
	298:		{"utf8mb4",	"utf8mb4_hr_0900_as_cs"},
	300:		{"utf8mb4",	"utf8mb4_vi_0900_as_cs"},
	303:		{"utf8mb4",	"utf8mb4_ja_0900_as_cs"},
	304:		{"utf8mb4",	"utf8mb4_ja_0900_as_cs_ks"},
	305:		{"utf8mb4",	"utf8mb4_0900_as_ci"},
	306:		{"utf8mb4",	"utf8mb4_ru_0900_ai_ci"},
	307:		{"utf8mb4",	"utf8mb4_ru_0900_as_cs"},
	308:		{"utf8mb4",	"utf8mb4_zh_0900_as_cs"},
	309:		{"utf8mb4",	"utf8mb4_0900_bin"},
}

//tuple (charset,collation id)
type charsetCollationID struct {
	charset string
	collationID int
}
//the map: collation name --> (charset,collationId)
//Run the SQL below in Mysql 8.0.23 to get the map.
//SQL: select concat('"',collation_name,'":\t\t{"',CHARACTER_SET_NAME,'",',ID,'},') from INFORMATION_SCHEMA.COLLATIONS order by collation_name;
var collationName2CharsetAndID = map[string]charsetCollationID{
	"armscii8_bin":		{"armscii8",64},
	"armscii8_general_ci":		{"armscii8",32},
	"ascii_bin":		{"ascii",65},
	"ascii_general_ci":		{"ascii",11},
	"big5_bin":		{"big5",84},
	"big5_chinese_ci":		{"big5",1},
	"binary":		{"binary",63},
	"cp1250_bin":		{"cp1250",66},
	"cp1250_croatian_ci":		{"cp1250",44},
	"cp1250_czech_cs":		{"cp1250",34},
	"cp1250_general_ci":		{"cp1250",26},
	"cp1250_polish_ci":		{"cp1250",99},
	"cp1251_bin":		{"cp1251",50},
	"cp1251_bulgarian_ci":		{"cp1251",14},
	"cp1251_general_ci":		{"cp1251",51},
	"cp1251_general_cs":		{"cp1251",52},
	"cp1251_ukrainian_ci":		{"cp1251",23},
	"cp1256_bin":		{"cp1256",67},
	"cp1256_general_ci":		{"cp1256",57},
	"cp1257_bin":		{"cp1257",58},
	"cp1257_general_ci":		{"cp1257",59},
	"cp1257_lithuanian_ci":		{"cp1257",29},
	"cp850_bin":		{"cp850",80},
	"cp850_general_ci":		{"cp850",4},
	"cp852_bin":		{"cp852",81},
	"cp852_general_ci":		{"cp852",40},
	"cp866_bin":		{"cp866",68},
	"cp866_general_ci":		{"cp866",36},
	"cp932_bin":		{"cp932",96},
	"cp932_japanese_ci":		{"cp932",95},
	"dec8_bin":		{"dec8",69},
	"dec8_swedish_ci":		{"dec8",3},
	"eucjpms_bin":		{"eucjpms",98},
	"eucjpms_japanese_ci":		{"eucjpms",97},
	"euckr_bin":		{"euckr",85},
	"euckr_korean_ci":		{"euckr",19},
	"gb18030_bin":		{"gb18030",249},
	"gb18030_chinese_ci":		{"gb18030",248},
	"gb18030_unicode_520_ci":		{"gb18030",250},
	"gb2312_bin":		{"gb2312",86},
	"gb2312_chinese_ci":		{"gb2312",24},
	"gbk_bin":		{"gbk",87},
	"gbk_chinese_ci":		{"gbk",28},
	"geostd8_bin":		{"geostd8",93},
	"geostd8_general_ci":		{"geostd8",92},
	"greek_bin":		{"greek",70},
	"greek_general_ci":		{"greek",25},
	"hebrew_bin":		{"hebrew",71},
	"hebrew_general_ci":		{"hebrew",16},
	"hp8_bin":		{"hp8",72},
	"hp8_english_ci":		{"hp8",6},
	"keybcs2_bin":		{"keybcs2",73},
	"keybcs2_general_ci":		{"keybcs2",37},
	"koi8r_bin":		{"koi8r",74},
	"koi8r_general_ci":		{"koi8r",7},
	"koi8u_bin":		{"koi8u",75},
	"koi8u_general_ci":		{"koi8u",22},
	"latin1_bin":		{"latin1",47},
	"latin1_danish_ci":		{"latin1",15},
	"latin1_general_ci":		{"latin1",48},
	"latin1_general_cs":		{"latin1",49},
	"latin1_german1_ci":		{"latin1",5},
	"latin1_german2_ci":		{"latin1",31},
	"latin1_spanish_ci":		{"latin1",94},
	"latin1_swedish_ci":		{"latin1",8},
	"latin2_bin":		{"latin2",77},
	"latin2_croatian_ci":		{"latin2",27},
	"latin2_czech_cs":		{"latin2",2},
	"latin2_general_ci":		{"latin2",9},
	"latin2_hungarian_ci":		{"latin2",21},
	"latin5_bin":		{"latin5",78},
	"latin5_turkish_ci":		{"latin5",30},
	"latin7_bin":		{"latin7",79},
	"latin7_estonian_cs":		{"latin7",20},
	"latin7_general_ci":		{"latin7",41},
	"latin7_general_cs":		{"latin7",42},
	"macce_bin":		{"macce",43},
	"macce_general_ci":		{"macce",38},
	"macroman_bin":		{"macroman",53},
	"macroman_general_ci":		{"macroman",39},
	"sjis_bin":		{"sjis",88},
	"sjis_japanese_ci":		{"sjis",13},
	"swe7_bin":		{"swe7",82},
	"swe7_swedish_ci":		{"swe7",10},
	"tis620_bin":		{"tis620",89},
	"tis620_thai_ci":		{"tis620",18},
	"ucs2_bin":		{"ucs2",90},
	"ucs2_croatian_ci":		{"ucs2",149},
	"ucs2_czech_ci":		{"ucs2",138},
	"ucs2_danish_ci":		{"ucs2",139},
	"ucs2_esperanto_ci":		{"ucs2",145},
	"ucs2_estonian_ci":		{"ucs2",134},
	"ucs2_general_ci":		{"ucs2",35},
	"ucs2_general_mysql500_ci":		{"ucs2",159},
	"ucs2_german2_ci":		{"ucs2",148},
	"ucs2_hungarian_ci":		{"ucs2",146},
	"ucs2_icelandic_ci":		{"ucs2",129},
	"ucs2_latvian_ci":		{"ucs2",130},
	"ucs2_lithuanian_ci":		{"ucs2",140},
	"ucs2_persian_ci":		{"ucs2",144},
	"ucs2_polish_ci":		{"ucs2",133},
	"ucs2_romanian_ci":		{"ucs2",131},
	"ucs2_roman_ci":		{"ucs2",143},
	"ucs2_sinhala_ci":		{"ucs2",147},
	"ucs2_slovak_ci":		{"ucs2",141},
	"ucs2_slovenian_ci":		{"ucs2",132},
	"ucs2_spanish2_ci":		{"ucs2",142},
	"ucs2_spanish_ci":		{"ucs2",135},
	"ucs2_swedish_ci":		{"ucs2",136},
	"ucs2_turkish_ci":		{"ucs2",137},
	"ucs2_unicode_520_ci":		{"ucs2",150},
	"ucs2_unicode_ci":		{"ucs2",128},
	"ucs2_vietnamese_ci":		{"ucs2",151},
	"ujis_bin":		{"ujis",91},
	"ujis_japanese_ci":		{"ujis",12},
	"utf16le_bin":		{"utf16le",62},
	"utf16le_general_ci":		{"utf16le",56},
	"utf16_bin":		{"utf16",55},
	"utf16_croatian_ci":		{"utf16",122},
	"utf16_czech_ci":		{"utf16",111},
	"utf16_danish_ci":		{"utf16",112},
	"utf16_esperanto_ci":		{"utf16",118},
	"utf16_estonian_ci":		{"utf16",107},
	"utf16_general_ci":		{"utf16",54},
	"utf16_german2_ci":		{"utf16",121},
	"utf16_hungarian_ci":		{"utf16",119},
	"utf16_icelandic_ci":		{"utf16",102},
	"utf16_latvian_ci":		{"utf16",103},
	"utf16_lithuanian_ci":		{"utf16",113},
	"utf16_persian_ci":		{"utf16",117},
	"utf16_polish_ci":		{"utf16",106},
	"utf16_romanian_ci":		{"utf16",104},
	"utf16_roman_ci":		{"utf16",116},
	"utf16_sinhala_ci":		{"utf16",120},
	"utf16_slovak_ci":		{"utf16",114},
	"utf16_slovenian_ci":		{"utf16",105},
	"utf16_spanish2_ci":		{"utf16",115},
	"utf16_spanish_ci":		{"utf16",108},
	"utf16_swedish_ci":		{"utf16",109},
	"utf16_turkish_ci":		{"utf16",110},
	"utf16_unicode_520_ci":		{"utf16",123},
	"utf16_unicode_ci":		{"utf16",101},
	"utf16_vietnamese_ci":		{"utf16",124},
	"utf32_bin":		{"utf32",61},
	"utf32_croatian_ci":		{"utf32",181},
	"utf32_czech_ci":		{"utf32",170},
	"utf32_danish_ci":		{"utf32",171},
	"utf32_esperanto_ci":		{"utf32",177},
	"utf32_estonian_ci":		{"utf32",166},
	"utf32_general_ci":		{"utf32",60},
	"utf32_german2_ci":		{"utf32",180},
	"utf32_hungarian_ci":		{"utf32",178},
	"utf32_icelandic_ci":		{"utf32",161},
	"utf32_latvian_ci":		{"utf32",162},
	"utf32_lithuanian_ci":		{"utf32",172},
	"utf32_persian_ci":		{"utf32",176},
	"utf32_polish_ci":		{"utf32",165},
	"utf32_romanian_ci":		{"utf32",163},
	"utf32_roman_ci":		{"utf32",175},
	"utf32_sinhala_ci":		{"utf32",179},
	"utf32_slovak_ci":		{"utf32",173},
	"utf32_slovenian_ci":		{"utf32",164},
	"utf32_spanish2_ci":		{"utf32",174},
	"utf32_spanish_ci":		{"utf32",167},
	"utf32_swedish_ci":		{"utf32",168},
	"utf32_turkish_ci":		{"utf32",169},
	"utf32_unicode_520_ci":		{"utf32",182},
	"utf32_unicode_ci":		{"utf32",160},
	"utf32_vietnamese_ci":		{"utf32",183},
	"utf8mb4_0900_ai_ci":		{"utf8mb4",255},
	"utf8mb4_0900_as_ci":		{"utf8mb4",305},
	"utf8mb4_0900_as_cs":		{"utf8mb4",278},
	"utf8mb4_0900_bin":		{"utf8mb4",309},
	"utf8mb4_bin":		{"utf8mb4",46},
	"utf8mb4_croatian_ci":		{"utf8mb4",245},
	"utf8mb4_cs_0900_ai_ci":		{"utf8mb4",266},
	"utf8mb4_cs_0900_as_cs":		{"utf8mb4",289},
	"utf8mb4_czech_ci":		{"utf8mb4",234},
	"utf8mb4_danish_ci":		{"utf8mb4",235},
	"utf8mb4_da_0900_ai_ci":		{"utf8mb4",267},
	"utf8mb4_da_0900_as_cs":		{"utf8mb4",290},
	"utf8mb4_de_pb_0900_ai_ci":		{"utf8mb4",256},
	"utf8mb4_de_pb_0900_as_cs":		{"utf8mb4",279},
	"utf8mb4_eo_0900_ai_ci":		{"utf8mb4",273},
	"utf8mb4_eo_0900_as_cs":		{"utf8mb4",296},
	"utf8mb4_esperanto_ci":		{"utf8mb4",241},
	"utf8mb4_estonian_ci":		{"utf8mb4",230},
	"utf8mb4_es_0900_ai_ci":		{"utf8mb4",263},
	"utf8mb4_es_0900_as_cs":		{"utf8mb4",286},
	"utf8mb4_es_trad_0900_ai_ci":		{"utf8mb4",270},
	"utf8mb4_es_trad_0900_as_cs":		{"utf8mb4",293},
	"utf8mb4_et_0900_ai_ci":		{"utf8mb4",262},
	"utf8mb4_et_0900_as_cs":		{"utf8mb4",285},
	"utf8mb4_general_ci":		{"utf8mb4",45},
	"utf8mb4_german2_ci":		{"utf8mb4",244},
	"utf8mb4_hr_0900_ai_ci":		{"utf8mb4",275},
	"utf8mb4_hr_0900_as_cs":		{"utf8mb4",298},
	"utf8mb4_hungarian_ci":		{"utf8mb4",242},
	"utf8mb4_hu_0900_ai_ci":		{"utf8mb4",274},
	"utf8mb4_hu_0900_as_cs":		{"utf8mb4",297},
	"utf8mb4_icelandic_ci":		{"utf8mb4",225},
	"utf8mb4_is_0900_ai_ci":		{"utf8mb4",257},
	"utf8mb4_is_0900_as_cs":		{"utf8mb4",280},
	"utf8mb4_ja_0900_as_cs":		{"utf8mb4",303},
	"utf8mb4_ja_0900_as_cs_ks":		{"utf8mb4",304},
	"utf8mb4_latvian_ci":		{"utf8mb4",226},
	"utf8mb4_la_0900_ai_ci":		{"utf8mb4",271},
	"utf8mb4_la_0900_as_cs":		{"utf8mb4",294},
	"utf8mb4_lithuanian_ci":		{"utf8mb4",236},
	"utf8mb4_lt_0900_ai_ci":		{"utf8mb4",268},
	"utf8mb4_lt_0900_as_cs":		{"utf8mb4",291},
	"utf8mb4_lv_0900_ai_ci":		{"utf8mb4",258},
	"utf8mb4_lv_0900_as_cs":		{"utf8mb4",281},
	"utf8mb4_persian_ci":		{"utf8mb4",240},
	"utf8mb4_pl_0900_ai_ci":		{"utf8mb4",261},
	"utf8mb4_pl_0900_as_cs":		{"utf8mb4",284},
	"utf8mb4_polish_ci":		{"utf8mb4",229},
	"utf8mb4_romanian_ci":		{"utf8mb4",227},
	"utf8mb4_roman_ci":		{"utf8mb4",239},
	"utf8mb4_ro_0900_ai_ci":		{"utf8mb4",259},
	"utf8mb4_ro_0900_as_cs":		{"utf8mb4",282},
	"utf8mb4_ru_0900_ai_ci":		{"utf8mb4",306},
	"utf8mb4_ru_0900_as_cs":		{"utf8mb4",307},
	"utf8mb4_sinhala_ci":		{"utf8mb4",243},
	"utf8mb4_sk_0900_ai_ci":		{"utf8mb4",269},
	"utf8mb4_sk_0900_as_cs":		{"utf8mb4",292},
	"utf8mb4_slovak_ci":		{"utf8mb4",237},
	"utf8mb4_slovenian_ci":		{"utf8mb4",228},
	"utf8mb4_sl_0900_ai_ci":		{"utf8mb4",260},
	"utf8mb4_sl_0900_as_cs":		{"utf8mb4",283},
	"utf8mb4_spanish2_ci":		{"utf8mb4",238},
	"utf8mb4_spanish_ci":		{"utf8mb4",231},
	"utf8mb4_sv_0900_ai_ci":		{"utf8mb4",264},
	"utf8mb4_sv_0900_as_cs":		{"utf8mb4",287},
	"utf8mb4_swedish_ci":		{"utf8mb4",232},
	"utf8mb4_tr_0900_ai_ci":		{"utf8mb4",265},
	"utf8mb4_tr_0900_as_cs":		{"utf8mb4",288},
	"utf8mb4_turkish_ci":		{"utf8mb4",233},
	"utf8mb4_unicode_520_ci":		{"utf8mb4",246},
	"utf8mb4_unicode_ci":		{"utf8mb4",224},
	"utf8mb4_vietnamese_ci":		{"utf8mb4",247},
	"utf8mb4_vi_0900_ai_ci":		{"utf8mb4",277},
	"utf8mb4_vi_0900_as_cs":		{"utf8mb4",300},
	"utf8mb4_zh_0900_as_cs":		{"utf8mb4",308},
	"utf8_bin":		{"utf8",83},
	"utf8_croatian_ci":		{"utf8",213},
	"utf8_czech_ci":		{"utf8",202},
	"utf8_danish_ci":		{"utf8",203},
	"utf8_esperanto_ci":		{"utf8",209},
	"utf8_estonian_ci":		{"utf8",198},
	"utf8_general_ci":		{"utf8",33},
	"utf8_general_mysql500_ci":		{"utf8",223},
	"utf8_german2_ci":		{"utf8",212},
	"utf8_hungarian_ci":		{"utf8",210},
	"utf8_icelandic_ci":		{"utf8",193},
	"utf8_latvian_ci":		{"utf8",194},
	"utf8_lithuanian_ci":		{"utf8",204},
	"utf8_persian_ci":		{"utf8",208},
	"utf8_polish_ci":		{"utf8",197},
	"utf8_romanian_ci":		{"utf8",195},
	"utf8_roman_ci":		{"utf8",207},
	"utf8_sinhala_ci":		{"utf8",211},
	"utf8_slovak_ci":		{"utf8",205},
	"utf8_slovenian_ci":		{"utf8",196},
	"utf8_spanish2_ci":		{"utf8",206},
	"utf8_spanish_ci":		{"utf8",199},
	"utf8_swedish_ci":		{"utf8",200},
	"utf8_tolower_ci":		{"utf8",76},
	"utf8_turkish_ci":		{"utf8",201},
	"utf8_unicode_520_ci":		{"utf8",214},
	"utf8_unicode_ci":		{"utf8",192},
	"utf8_vietnamese_ci":		{"utf8",215},
}

//mysql client capability
const (
	CLIENT_LONG_PASSWORD 					uint32 = 0x00000001
	CLIENT_FOUND_ROWS	 					uint32 = 0x00000002
	CLIENT_LONG_FLAG	 					uint32 = 0x00000004
	CLIENT_CONNECT_WITH_DB 					uint32 = 0x00000008
	CLIENT_NO_SCHEMA						uint32 = 0x00000010
	CLIENT_COMPRESS     					uint32 = 0x00000020
	CLIENT_LOCAL_FILES 						uint32 = 0x00000080
	CLIENT_IGNORE_SPACE 					uint32 = 0x00000100
	CLIENT_PROTOCOL_41 						uint32 = 0x00000200
	CLIENT_INTERACTIVE 						uint32 = 0x00000400
	CLIENT_SSL 								uint32 = 0x00000800
	CLIENT_IGNORE_SIGPIPE 					uint32 = 0x00001000
	CLIENT_TRANSACTIONS 					uint32 = 0x00002000
	CLIENT_RESERVED 						uint32 = 0x00004000
	CLIENT_SECURE_CONNECTION 				uint32 = 0x00008000
	CLIENT_MULTI_STATEMENTS 				uint32 = 0x00010000
	CLIENT_MULTI_RESULTS 					uint32 = 0x00020000
	CLIENT_PS_MULTI_RESULTS 				uint32 = 0x00040000
	CLIENT_PLUGIN_AUTH 						uint32 = 0x00080000
	CLIENT_CONNECT_ATTRS 					uint32 = 0x00100000
	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA 	uint32 = 0x00200000
	CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS 	uint32 = 0x00400000
	CLIENT_SESSION_TRACK 					uint32 = 0x00800000
	CLIENT_DEPRECATE_EOF 					uint32 = 0x01000000
)

//server status
const (
	SERVER_STATUS_IN_TRANS				uint16 = 0x0001
	SERVER_STATUS_AUTOCOMMIT			uint16 = 0x0002
	SERVER_MORE_RESULTS_EXISTS			uint16 = 0x0008
	SERVER_STATUS_NO_GOOD_INDEX_USED	uint16 = 0x0010
	SERVER_STATUS_NO_INDEX_USED			uint16 = 0x0020
	SERVER_STATUS_CURSOR_EXISTS			uint16 = 0x0040
	SERVER_STATUS_LAST_ROW_SENT			uint16 = 0x0080
	SERVER_STATUS_DB_DROPPED			uint16 = 0x0100
	SERVER_STATUS_NO_BACKSLASH_ESCAPES	uint16 = 0x0200
	SERVER_STATUS_METADATA_CHANGED		uint16 = 0x0400
	SERVER_QUERY_WAS_SLOW				uint16 = 0x0800
	SERVER_PS_OUT_PARAMS				uint16 = 0x1000
	SERVER_STATUS_IN_TRANS_READONLY		uint16 = 0x2000
	SERVER_SESSION_STATE_CHANGED		uint16 = 0x4000
)

//text protocol in mysql client protocol
//iteration command
const(
	COM_SLEEP 				uint8 = 0x00
	COM_QUIT 				uint8 = 0x01
	COM_INIT_DB				uint8 = 0x02
	COM_QUERY				uint8 = 0x03
	COM_FIELD_LIST			uint8 = 0x04
	COM_CREATE_DB			uint8 = 0x05
	COM_DROP_DB				uint8 = 0x06
	COM_REFRESH				uint8 = 0x07
	COM_SHUTDOWN			uint8 = 0x08
	COM_STATISTICS			uint8 = 0x09
	COM_PROCESS_INFO		uint8 = 0x0a
	COM_CONNECT				uint8 = 0x0b
	COM_PROCESS_KILL		uint8 = 0x0c
	COM_DEBUG				uint8 = 0x0d
	COM_PING				uint8 = 0x0e
	COM_TIME				uint8 = 0x0f
	COM_DELAYED_INSERT		uint8 = 0x10
	COM_CHANGE_USER			uint8 = 0x11
	COM_STMT_PREPARE		uint8 = 0x16
	COM_STMT_EXECUTE		uint8 = 0x17
	COM_STMT_SEND_LONG_DATA	uint8 = 0x18
	COM_STMT_CLOSE			uint8 = 0x19
	COM_STMT_RESET			uint8 = 0x1a
	COM_SET_OPTION			uint8 = 0x1b
	COM_STMT_FETCH			uint8 = 0x1c
	COM_DAEMON				uint8 = 0x1d
	COM_RESET_CONNECTION	uint8 = 0x1f
)

// DefaultCapability means default capability of the server
var DefaultCapability uint32 = CLIENT_LONG_PASSWORD |
	CLIENT_LONG_FLAG |
	CLIENT_CONNECT_WITH_DB |
	CLIENT_PROTOCOL_41 |
	CLIENT_TRANSACTIONS |
	CLIENT_SECURE_CONNECTION |
	CLIENT_PLUGIN_AUTH

//default server status
var DefaultClientConnStatus uint16 = SERVER_STATUS_AUTOCOMMIT

const (
	serverVersion               = "8.0.32-MatrixOne"
	clientProtocolVersion uint8 = 10

	/**
	An answer talks about the charset utf8mb4.
	https://stackoverflow.com/questions/766809/whats-the-difference-between-utf8-general-ci-and-utf8-unicode-ci
	It recommends the charset utf8mb4_0900_ai_ci.
	Maybe we can support utf8mb4_0900_ai_ci in the future.

	A concise research in the Mysql 8.0.23.

	the charset in sever level
	======================================

	mysql> show variables like 'character_set_server';
	+----------------------+---------+
	| Variable_name        | Value   |
	+----------------------+---------+
	| character_set_server | utf8mb4 |
	+----------------------+---------+

	mysql> show variables like 'collation_server';
	+------------------+--------------------+
	| Variable_name    | Value              |
	+------------------+--------------------+
	| collation_server | utf8mb4_0900_ai_ci |
	+------------------+--------------------+

	the charset in database level
	=====================================
	mysql> show variables like 'character_set_database';
	+------------------------+---------+
	| Variable_name          | Value   |
	+------------------------+---------+
	| character_set_database | utf8mb4 |
	+------------------------+---------+

	mysql> show variables like 'collation_database';
	+--------------------+--------------------+
	| Variable_name      | Value              |
	+--------------------+--------------------+
	| collation_database | utf8mb4_0900_ai_ci |
	+--------------------+--------------------+

	*/
	// DefaultCollationID is utf8mb4_bin(46)
	utf8mb4BinCollationID uint8 = 46

	utf8mb4CollationID	uint8 = 45

	AuthNativePassword				  = "mysql_native_password"

	//If the payload is larger than or equal to 224−1 bytes the length is set to 224−1 (ff ff ff)
	//and additional packets are sent with the rest of the payload until the payload of a packet
	//is less than 224−1 bytes.
	MaxPayloadSize         uint32 = (1 << 24) -1

	// DefaultMySQLState is default state of the mySQL
	DefaultMySQLState = "HY000"

	//Err code in mysql
	ErrUnknown             uint16= 1105
)

type MysqlClientProtocol struct {
	ClientProtocolImpl

	//The sequence-id is incremented with each packet and may wrap around.
	//It starts at 0 and is reset to 0 when a new command begins in the Command Phase.
	sequenceId uint8

	//collation id
	collationID int

	//collation name
	collationName string

	//character set
	charset string

	//joint capability shared by the server and the client
	capability uint32
}

//handshake response 41
type response41 struct {
	capability uint32
	maxClientPacketSize uint32
	collationID uint8
	username string
	authResponse []byte
	database string
	authPluginName string
}
//handshake response 320
type response320 struct {
	capability uint32
	maxClientPacketSize uint32
	//collationID uint8
	username string
	authResponse []byte
	database string
	//authPluginName string
}


//read an int with length encoded from the buffer at the position
//return the int ; position + the count of bytes for length encoded (1 or 3 or 4 or 9)
func (mcp *MysqlClientProtocol) readIntLenEnc(data []byte, pos int) (uint64, int, bool) {
	if pos >= len(data) {
		return 0, 0, false
	}
	switch data[pos] {
	case 0xfb:
		//zero, one byte
		return 0, pos + 1, true
	case 0xfc:
		// int in two bytes
		if pos+2 >= len(data) {
			return 0, 0, false
		}
		value := uint64(data[pos+1]) |
			uint64(data[pos+2])<<8
		return value, pos + 3, true
	case 0xfd:
		// int in three bytes
		if pos+3 >= len(data) {
			return 0, 0, false
		}
		value := uint64(data[pos+1]) |
			uint64(data[pos+2])<<8 |
			uint64(data[pos+3])<<16
		return value, pos + 4,  true
	case 0xfe:
		// int in eight bytes
		if pos+8 >= len(data) {
			return 0, 0, false
		}
		value := uint64(data[pos+1]) |
			uint64(data[pos+2])<<8 |
			uint64(data[pos+3])<<16 |
			uint64(data[pos+4])<<24 |
			uint64(data[pos+5])<<32 |
			uint64(data[pos+6])<<40 |
			uint64(data[pos+7])<<48 |
			uint64(data[pos+8])<<56
		return value, pos + 9, true
	}
	// 0-250
	return uint64(data[pos]), pos + 1, true
}

//write an int with length encoded into the buffer at the position
//return position + the count of bytes for length encoded (1 or 3 or 4 or 9)
func (mcp *MysqlClientProtocol) writeIntLenEnc(data []byte,pos int,value uint64) int  {
	switch {
	case value < 251:
		data[pos] = byte(value)
		return pos + 1
	case value < (1 << 16):
		data[pos] = 0xfc
		data[pos+1] = byte(value)
		data[pos+2] = byte(value >> 8)
		return pos + 3
	case value < (1 << 24):
		data[pos] = 0xfd
		data[pos+1] = byte(value)
		data[pos+2] = byte(value >> 8)
		data[pos+3] = byte(value >> 16)
		return pos + 4
	default:
		data[pos] = 0xfe
		data[pos+1] = byte(value)
		data[pos+2] = byte(value >> 8)
		data[pos+3] = byte(value >> 16)
		data[pos+4] = byte(value >> 24)
		data[pos+5] = byte(value >> 32)
		data[pos+6] = byte(value >> 40)
		data[pos+7] = byte(value >> 48)
		data[pos+8] = byte(value >> 56)
		return pos + 9
	}
}

//read the count of bytes from the buffer at the position
//return bytes slice ; position + count ; true - succeeded or false - failed
func (mcp *MysqlClientProtocol) readCountOfBytes(data []byte,pos int,count int)([]byte,int,bool)  {
	if pos+count-1 >= len(data) {
		return nil, 0, false
	}
	return data[pos : pos + count], pos + count, true
}

//write the count of bytes into the buffer at the position
//return position + the number of bytes
func (mcp *MysqlClientProtocol) writeCountOfBytes(data []byte,pos int,value []byte)int  {
	pos += copy(data[pos:],value)
	return pos
}

//read a string with fixed length from the buffer at the position
//return string ; position + length ; true - succeeded or false - failed
func (mcp *MysqlClientProtocol) readStringFix(data []byte,pos int,length int)(string,int,bool)  {
	var sdata []byte
	var ok bool
	sdata,pos,ok = mcp.readCountOfBytes(data,pos,length)
	if !ok {
		return "", 0, false
	}
	return string(sdata),pos,true
}

//write a string with fixed length into the buffer at the position
//return pos + string.length
func (mcp *MysqlClientProtocol) writeStringFix(data []byte, pos int, value string,length int) int {
	pos += copy(data[pos:],value[0:length])
	return pos
}

//read a string appended with zero from the buffer at the position
//return string ; position + length of the string + 1; true - succeeded or false - failed
func (mcp *MysqlClientProtocol) readStringNUL(data []byte, pos int) (string, int, bool) {
	zeroPos := bytes.IndexByte(data[pos:], 0)
	if zeroPos == -1 {
		return "", 0, false
	}
	return string(data[pos : pos+zeroPos]), pos + zeroPos + 1, true
}

//write a string into the buffer at the position, then appended with 0
//return pos + string.length + 1
func (mcp *MysqlClientProtocol) writeStringNUL(data []byte,pos int,value string) int {
	pos = mcp.writeStringFix(data,pos,value,len(value))
	data[pos] = 0
	return pos + 1
}

//read a string with length encoded from the buffer at the position
//return string ; position + the count of bytes for length encoded (1 or 3 or 4 or 9) + length of the string; true - succeeded or false - failed
func (mcp *MysqlClientProtocol) readStringLenEnc(data []byte,pos int) (string,int,bool) {
	var value uint64
	var ok bool
	value, pos, ok = mcp.readIntLenEnc(data, pos)
	if !ok {
		return "", 0, false
	}
	sLength := int(value)
	if pos +sLength- 1 >= len(data) {
		return "", 0, false
	}
	return string(data[pos : pos+sLength]), pos + sLength, true
}

//write a string with length encoded into the buffer at the position
//return position + the count of bytes for length encoded (1 or 3 or 4 or 9) + length of the string;
func (mcp *MysqlClientProtocol) writeStringLenEnc(data []byte,pos int,value string) int {
	pos = mcp.writeIntLenEnc(data, pos, uint64(len(value)))
	return mcp.writeStringFix(data, pos, value,len(value))
}

//write the count of zeros into the buffer at the position
//return pos + count
func (mcp *MysqlClientProtocol) writeZeros(data []byte, pos int, count int) int {
	for i := 0; i < count; i++ {
		data[pos+i] = 0
	}
	return pos + count
}

//the server calculates the hash value of the password with the algorithm
//and judges it with the authentication data from the client.
//Algorithm: SHA1( password ) XOR SHA1( slat + SHA1( SHA1( password ) ) )
func (mcp *MysqlClientProtocol) checkPassword(password,salt,auth []byte) bool{
	if len(password) == 0{
		return false
	}
	//hash1 = SHA1(password)
	sha := sha1.New()
	_,err := sha.Write(password)
	if err!= nil{
		fmt.Printf("SHA1(password) failed.")
		return false
	}
	hash1 := sha.Sum(nil)

	//hash2 = SHA1(SHA1(password))
	sha.Reset()
	_,err = sha.Write(hash1)
	if err!= nil{
		fmt.Printf("SHA1(SHA1(password)) failed.")
		return false
	}
	hash2:=sha.Sum(nil)

	//hash3 = SHA1(salt + SHA1(SHA1(password)))
	sha.Reset()
	_,err = sha.Write(salt)
	if err!= nil{
		fmt.Printf("write salt failed.")
		return false
	}
	_,err = sha.Write(hash2)
	if err!= nil{
		fmt.Printf("write SHA1(SHA1(password)) failed.")
		return false
	}
	hash3 := sha.Sum(nil)

	//SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))
	for i := range hash1 {
		hash1[i] ^= hash3[i]
	}

	fmt.Printf("server calculated %v\n",hash1)
	fmt.Printf("client calculated %v\n",auth)

	return bytes.Equal(hash1,auth)
}

func (mcp *MysqlClientProtocol) setSequenceID(value uint8)  {
	mcp.sequenceId = value
}

//the server makes a handshake v10 packet
//return handshake packet
func (mcp *MysqlClientProtocol) makeHandshakePacketV10() []byte {
	var data=make([]byte,256)
	var pos int = 0
	//int<1> protocol version
	pos = mcp.io.WriteUint8(data,pos, clientProtocolVersion)

	//string[NUL] server version
	pos = mcp.writeStringNUL(data,pos, serverVersion)

	//int<4> connection id
	pos = mcp.io.WriteUint32(data,pos,mcp.connectionID)

	//string[8] auth-plugin-data-part-1
	pos = mcp.writeCountOfBytes(data,pos,mcp.salt[0:8])

	//int<1> filler 0
	pos = mcp.io.WriteUint8(data,pos,0)

	//int<2>              capability flags (lower 2 bytes)
	pos = mcp.io.WriteUint16(data,pos,uint16(DefaultCapability & 0xFFFF))

	//int<1>              character set
	pos = mcp.io.WriteUint8(data,pos, utf8mb4BinCollationID)

	//int<2>              status flags
	pos = mcp.io.WriteUint16(data,pos,DefaultClientConnStatus)

	//int<2>              capability flags (upper 2 bytes)
	pos = mcp.io.WriteUint16(data,pos,uint16((DefaultCapability >> 16) & 0xFFFF))

	if (DefaultCapability & CLIENT_PLUGIN_AUTH) != 0{
		//int<1>              length of auth-plugin-data
		//set 21 always
		pos = mcp.io.WriteUint8(data,pos,21)
	}else{
		//int<1>              [00]
		//set 0 always
		pos = mcp.io.WriteUint8(data,pos,0)
	}

	//string[10]     reserved (all [00])
	pos = mcp.writeZeros(data,pos,10)

	if (DefaultCapability & CLIENT_SECURE_CONNECTION) != 0{
		//string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
		pos = mcp.writeCountOfBytes(data,pos,mcp.salt[8:])
		pos = mcp.io.WriteUint8(data,pos,0)
	}

	if (DefaultCapability & CLIENT_PLUGIN_AUTH) != 0{
		//string[NUL]    auth-plugin name
		pos = mcp.writeStringNUL(data,pos,AuthNativePassword)
	}

	return data[:pos]
}

//the server analyses handshake response41 info from the client
//return true - analysed successfully / false - failed ; response41 ; error
func (mcp *MysqlClientProtocol) analyseHandshakeResponse41(data []byte) (bool,response41,error) {
	var pos int = 0
	var ok bool
	var info response41

	//int<4>             capability flags of the client, CLIENT_PROTOCOL_41 always set
	info.capability,pos,ok = mcp.io.ReadUint32(data,pos)
	if !ok{
		return false,info,fmt.Errorf("get capability failed")
	}

	if (info.capability & CLIENT_PROTOCOL_41) == 0{
		return false,info,fmt.Errorf("capability does not have protocol 41")
	}

	//int<4>             max-packet size
	//max size of a command packet that the client wants to send to the server
	info.maxClientPacketSize,pos,ok = mcp.io.ReadUint32(data,pos)
	if !ok{
		return false,info,fmt.Errorf("get max packet size failed")
	}

	//int<1>             character set
	//connection's default character set
	info.collationID,pos,ok = mcp.io.ReadUint8(data,pos)
	if !ok{
		return false,info,fmt.Errorf("get character set failed")
	}

	//string[23]         reserved (all [0])
	//just skip it
	pos += 23

	//string[NUL]        username
	info.username,pos,ok = mcp.readStringNUL(data,pos)
	if !ok{
		return false,info,fmt.Errorf("get username failed")
	}

	/*
		if capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA {
			lenenc-int         length of auth-response
			string[n]          auth-response
		} else if capabilities & CLIENT_SECURE_CONNECTION {
			int<1>             length of auth-response
			string[n]           auth-response
		} else {
			string[NUL]        auth-response
		}
	*/
	if (info.capability & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0{
		var l uint64
		l,pos,ok = mcp.readIntLenEnc(data,pos)
		if !ok{
			return false,info,fmt.Errorf("get length of auth-response failed")
		}
		info.authResponse,pos,ok = mcp.readCountOfBytes(data,pos,int(l))
		if !ok{
			return false,info,fmt.Errorf("get auth-response failed")
		}
	}else if (info.capability & CLIENT_SECURE_CONNECTION) != 0 {
		var l uint8
		l,pos,ok = mcp.io.ReadUint8(data,pos)
		if !ok{
			return false,info,fmt.Errorf("get length of auth-response failed")
		}
		info.authResponse,pos,ok = mcp.readCountOfBytes(data,pos,int(l))
		if !ok{
			return false,info,fmt.Errorf("get auth-response failed")
		}
	}else{
		var auth string
		auth,pos,ok = mcp.readStringNUL(data,pos)
		if !ok{
			return false,info,fmt.Errorf("get auth-response failed")
		}
		info.authResponse = []byte(auth)
	}

	if (info.capability & CLIENT_CONNECT_WITH_DB) != 0{
		info.database,pos,ok = mcp.readStringNUL(data,pos)
		if !ok{
			return false,info,fmt.Errorf("get database failed")
		}
	}

	if (info.capability & CLIENT_PLUGIN_AUTH) != 0 {
		info.authPluginName,pos,ok = mcp.readStringNUL(data,pos)
		if !ok{
			return false,info,fmt.Errorf("get auth plugin name failed")
		}

		//to switch authenticate method
		if info.authPluginName != AuthNativePassword{
			var err error
			if info.authResponse,err = mcp.negotiateAuthenticationMethod();err != nil{
				return false,info,fmt.Errorf("negotiate authentication method failed. error:%v",err)
			}
			info.authPluginName = AuthNativePassword
		}
	}

	//drop client connection attributes
	return true,info,nil
}

//the server does something after receiving a handshake response41 from the client
//like check user and password
//and other things
func (mcp *MysqlClientProtocol) handleClientResponse41(resp41 response41) error {
	//to do something else
	fmt.Printf("capability 0x%x\n",resp41.capability)
	fmt.Printf("maxClientPacketSize %d\n",resp41.maxClientPacketSize)
	fmt.Printf("collationID %d\n",resp41.collationID)
	fmt.Printf("username %s\n",resp41.username)
	fmt.Printf("authResponse: \n")
	//update the capability with client's capability
	mcp.capability = defaultCapability & resp41.capability

	//TO Check password
	psw := "111"
	if mcp.checkPassword([]byte(psw),mcp.salt,resp41.authResponse) {
		fmt.Printf("check password succeeded\n")
	}else{
		return fmt.Errorf("check password failed\n")
	}

	//character set
	if nameAndCharset,ok := collationID2CharsetAndName[int(resp41.collationID)]; !ok {
		return fmt.Errorf("get collationName and charset failed")
	}else{
		mcp.collationID = int(resp41.collationID)
		mcp.collationName = nameAndCharset.collationName
		mcp.charset = nameAndCharset.charset
	}

	fmt.Printf("collationID %d collatonName %s charset %s \n",mcp.collationID,mcp.collationName,mcp.charset)
	fmt.Printf("database %s \n",resp41.database)
	fmt.Printf("authPluginName %s \n",resp41.authPluginName)
	return nil
}

//the server analyses handshake response320 info from the old client
//return true - analysed successfully / false - failed ; response320 ; error
func (mcp *MysqlClientProtocol) analyseHandshakeResponse320(data []byte) (bool,response320,error){
	var pos int = 0
	var ok bool
	var info response320
	var capa uint16

	//int<2>             capability flags, CLIENT_PROTOCOL_41 never set
	capa,pos,ok = mcp.io.ReadUint16(data,pos)
	if !ok{
		return false,info,fmt.Errorf("get capability failed")
	}
	info.capability = uint32(capa)

	//int<3>             max-packet size
	//max size of a command packet that the client wants to send to the server
	info.maxClientPacketSize = uint32(data[pos]) | uint32(data[pos+1]) << 8 | uint32(data[pos+2]) << 16
	pos += 3

	//string[NUL]        username
	info.username,pos,ok = mcp.readStringNUL(data,pos)
	if !ok{
		return false,info,fmt.Errorf("get username failed")
	}

	if (info.capability & CLIENT_CONNECT_WITH_DB) != 0{
		var auth string
		auth,pos,ok = mcp.readStringNUL(data,pos)
		if !ok{
			return false,info,fmt.Errorf("get auth-response failed")
		}
		info.authResponse = []byte(auth)

		info.database,pos,ok = mcp.readStringNUL(data,pos)
		if !ok{
			return false,info,fmt.Errorf("get database failed")
		}
	}else{
		info.authResponse,pos,ok = mcp.readCountOfBytes(data,pos,len(data) - pos)
		if !ok{
			return false,info,fmt.Errorf("get auth-response failed")
		}
	}

	return true,info,nil
}

//the server does something after receiving a handshake response320 from the client
//like check user and password
//and other things
func (mcp *MysqlClientProtocol) handleClientResponse320(resp320 response320) error {
	//to do something else
	fmt.Printf("capability 0x%x\n",resp320.capability)
	fmt.Printf("maxClientPacketSize %d\n",resp320.maxClientPacketSize)
	fmt.Printf("username %s\n",resp320.username)
	fmt.Printf("authResponse: \n")

	//update the capability with client's capability
	mcp.capability = defaultCapability & resp320.capability

	//TO Check password
	psw := "111"
	if mcp.checkPassword([]byte(psw),mcp.salt,resp320.authResponse) {
		fmt.Printf("check password succeeded\n")
	}else{
		return fmt.Errorf("check password failed\n")
	}

	//if the client does not notice its default charset, the server gives a default charset.
	//Run the sql in mysql 8.0.23 to get the charset
	//the sql: select * from information_schema.collations where collation_name = 'utf8mb4_general_ci';
	mcp.collationID = int(utf8mb4CollationID)
	mcp.collationName = "utf8mb4_general_ci"
	mcp.charset = "utf8mb4"

	fmt.Printf("collationID %d collatonName %s charset %s \n",mcp.collationID,mcp.collationName,mcp.charset)
	fmt.Printf("database %s \n",resp320.database)
	return nil
}

//the server makes a AuthSwitchRequest that asks the client to authenticate the data with new method
func (mcp *MysqlClientProtocol) makeAuthSwitchRequest(authMethodName string) []byte {
	var data=make([]byte,1+len(authMethodName)+1+len(mcp.salt)+1)
	pos := mcp.io.WriteUint8(data,0,0xFE)
	pos = mcp.writeStringNUL(data,pos,authMethodName)
	pos = mcp.writeCountOfBytes(data,pos,mcp.salt)
	pos = mcp.io.WriteUint8(data,pos,0)
	return data
}

//the server can send AuthSwitchRequest to ask client to use designated authentication method,
//if both server and client support CLIENT_PLUGIN_AUTH capability.
//return data authenticated with new method
func (mcp *MysqlClientProtocol) negotiateAuthenticationMethod() ([]byte,error) {
	var err error
	var data []byte
	aswPkt:=mcp.makeAuthSwitchRequest(AuthNativePassword)
	if err = mcp.sendPayload(aswPkt);err!=nil{
		return nil,fmt.Errorf("send AuthSwitchRequest failed. error:%v",err)
	}

	if data,err = mcp.recvPayload(); err != nil{
		return nil,fmt.Errorf("recv AuthSwitchResponse failed. error:%v",err)
	}

	return data,nil
}

//make a OK packet
func (mcp *MysqlClientProtocol) makeOKPacket(affectedRows, lastInsertId uint64, statusFlags,warnings uint16) []byte {
	var data=make([]byte,128)
	var pos int = 0
	pos = mcp.io.WriteUint8(data,pos,0)
	pos = mcp.writeIntLenEnc(data,pos,affectedRows)
	pos = mcp.writeIntLenEnc(data,pos,lastInsertId)
	if (defaultCapability & CLIENT_PROTOCOL_41) != 0{
		pos =mcp.io.WriteUint16(data,pos,statusFlags)
		pos =mcp.io.WriteUint16(data,pos,warnings)
	}else{
		pos =mcp.io.WriteUint16(data,pos,statusFlags)
	}
	return data
}

//send OK packet to the client
func (mcp *MysqlClientProtocol) sendOKPacket(status uint16) error {
	okPkt := mcp.makeOKPacket(0,0,status,0)
	if err := mcp.sendPayload(okPkt);err != nil{
		return fmt.Errorf("send ok packet failed.error:%v",err)
	}
	return nil
}

//make Err packet
func (mcp *MysqlClientProtocol) makeErrPacket(errorCode uint16, sqlState, errorMessage string) []byte {
	var data = make([]byte,9+len(errorMessage))
	pos := mcp.io.WriteUint8(data,0,0xff)
	pos = mcp.io.WriteUint16(data,pos,errorCode)
	if mcp.capability & CLIENT_PROTOCOL_41 != 0 {
		pos = mcp.io.WriteUint8(data,pos,'#')
		pos = mcp.writeStringFix(data,pos,sqlState,5)
	}
	pos = mcp.writeStringFix(data,pos,errorMessage,len(errorMessage))
	return data
}

//the server sends the Err packet
func (mcp *MysqlClientProtocol) sendErrPacket(errorCode uint16, sqlState, errorMessage string) error {
	errPkt := mcp.makeErrPacket(errorCode,sqlState,errorMessage)
	if err := mcp.sendPayload(errPkt);err != nil{
		return fmt.Errorf("send err packet failed.error:%v",err)
	}
	return nil
}

//the server sends the payload to the client
func (mcp *MysqlClientProtocol) sendPayload(data []byte)error  {
	var i int = 0
	var length int = len(data)
	var	curLen int = 0
	for ;i < length ; i += curLen{
		curLen = min(int(MaxPayloadSize),length)

		//make mysql client protocol header
		//4 bytes
		//int<3>    the length of payload
		var header [4]byte
		mcp.io.WriteUint32(header[:],0,uint32(curLen))

		//int<1> sequence id
		mcp.io.WriteUint8(header[:],3,mcp.sequenceId)

		//send header
		if err := mcp.io.WritePacket(header[:]); err != nil{
			return fmt.Errorf("write header failed. error:%v",err)
		}

		//send payload
		if err := mcp.io.WritePacket(data[i:i+curLen]);err != nil{
			return fmt.Errorf("write payload failed. error:%v",err)
		}

		mcp.sequenceId++

		if i+curLen == length && curLen == int(MaxPayloadSize) {
			//if the size of the last packet is exactly MaxPayloadSize, a zero-size payload should be sent
			header[0] = 0
			header[1] = 0
			header[2] = 0
			header[3] = mcp.sequenceId

			//send header / zero-sized packet
			if err := mcp.io.WritePacket(header[:]); err != nil{
				return fmt.Errorf("write header failed. error:%v",err)
			}

			mcp.sequenceId++
		}
	}
	mcp.io.Flush()
	return nil
}


//ther server reads a part of payload from the connection
//the part may be a whole payload
func (mcp *MysqlClientProtocol) recvPartOfPayload() ([]byte, error) {
	var length int
	var header []byte
	var err error
	if header,err = mcp.io.ReadPacket(4); err!=nil{
		return nil,fmt.Errorf("read header failed.error:%v",err)
	}else if header[3] != mcp.sequenceId{
		return nil,fmt.Errorf("client sequence id %d != server sequence id %d",header[3],mcp.sequenceId)
	}

	mcp.sequenceId++
	length = int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

	var payload []byte
	if payload,err = mcp.io.ReadPacket(length); err!=nil{
		return nil,fmt.Errorf("read payload failed.error:%v",err)
	}
	return payload,nil
}

//the server read a payload from the connection
func (mcp *MysqlClientProtocol) recvPayload() ([]byte, error) {
	payload,err :=mcp.recvPartOfPayload()
	if err!=nil{
		return nil,err
	}

	//only one part
	if len(payload) < int(MaxPayloadSize) {
		return payload,nil
	}

	//payload has been split into many parts.
	//read them all together
	var part []byte
	for{
		part,err =mcp.recvPartOfPayload()
		if err!=nil{
			return nil,err
		}

		payload = append(payload,part...)

		//only one part
		if len(part) < int(MaxPayloadSize) {
			break
		}
	}
	return payload,nil
}



func (mcp *MysqlClientProtocol) Handshake() error {
	hsV10pkt :=mcp.makeHandshakePacketV10()
	if err := mcp.sendPayload(hsV10pkt);err != nil{
		return fmt.Errorf("send handshake v10 packet failed.error:%v",err)
	}

	var payload []byte
	var err error
	if payload,err = mcp.recvPayload(); err!=nil{
		return err
	}

	if len(payload) < 2{
		return fmt.Errorf("broken response packet failed")
	}

	if capability,_,ok := mcp.io.ReadUint16(payload,0); !ok{
		return fmt.Errorf("read capability from response packet failed")
	}else if uint32(capability) & CLIENT_PROTOCOL_41 != 0{
		//analyse response41
		var resp41 response41
		var ok bool
		if ok,resp41,err = mcp.analyseHandshakeResponse41(payload);!ok{
			return err
		}

		if err = mcp.handleClientResponse41(resp41); err != nil{
			return err
		}
	}else{
		//analyse response320
		var resp320 response320
		var ok bool
		if ok,resp320,err = mcp.analyseHandshakeResponse320(payload);!ok{
			return err
		}

		if err = mcp.handleClientResponse320(resp320); err != nil{
			return err
		}
	}



	if err = mcp.sendOKPacket(0); err!=nil{
		return err
	}
	return nil
}

func (mcp *MysqlClientProtocol) ReadRequest()(*Request,error)  {
	var payload []byte
	var err error
	if payload,err = mcp.recvPayload(); err!=nil{
		return nil,err
	}

	req := &Request{
		cmd: int(payload[0]),
		data: payload[1:],
	}

	return req,nil
}

func (mcp *MysqlClientProtocol) SendResponse(resp *Response) error  {
	switch resp.category {
	case okResponse:
		return mcp.sendOKPacket(uint16(resp.status))
	default:
		return fmt.Errorf("unsupported response:%d ",resp.category)
	}
	return nil
}

func min(a int, b int) int{
	if a < b {
		return a
	}else{
		return b
	}
}

func max(a int, b int) int{
	if a < b {
		return b
	}else{
		return a
	}
}

func NewMysqlClientProtocol(IO IOPackage,connectionID uint32) *MysqlClientProtocol{
	rand.Seed(time.Now().UTC().UnixNano())
	salt := make([]byte, 20)
	rand.Read(salt)

	mysql := &MysqlClientProtocol{
		ClientProtocolImpl: ClientProtocolImpl{
			io:IO,
			salt:salt,
			connectionID:connectionID,
		},
		sequenceId: 0,
		charset: "utf8mb4",
		capability: defaultCapability,
	}
	return mysql
}