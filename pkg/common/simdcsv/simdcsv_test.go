/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package simdcsv

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"math/bits"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"unicode/utf8"
)

//
// Below are the test cases from `encoding/csv`.
//
// They are copied directly from https://golang.org/src/encoding/csv/reader_test.go
//
func TestRead(t *testing.T) {
	tests := []struct {
		Name   string
		Input  string
		Output [][]string
		Error  error

		// These fields are copied into the Reader
		Comma              rune
		Comment            rune
		UseFieldsPerRecord bool // false (default) means FieldsPerRecord is -1
		FieldsPerRecord    int
		LazyQuotes         bool
		TrimLeadingSpace   bool
		ReuseRecord        bool
	}{{
		Name:   "Simple",
		Input:  "a,b,c\n",
		Output: [][]string{{"a", "b", "c"}},
	}, {
		Name:   "CRLF",
		Input:  "a,b\r\nc,d\r\n",
		Output: [][]string{{"a", "b"}, {"c", "d"}},
	}, {
		Name:   "BareCR",
		Input:  "a,b\rc,d\r\n",
		Output: [][]string{{"a", "b\rc", "d"}},
	}, {
		Name: "RFC4180test",
		Input: `#field1,field2,field3
"aaa","bb
b","ccc"
"a,a","b""bb","ccc"
zzz,yyy,xxx
`,
		Output: [][]string{
			{"#field1", "field2", "field3"},
			{"aaa", "bb\nb", "ccc"},
			{"a,a", `b"bb`, "ccc"},
			{"zzz", "yyy", "xxx"},
		},
		UseFieldsPerRecord: true,
		FieldsPerRecord:    0,
	}, {
		Name:   "NoEOLTest",
		Input:  "a,b,c",
		Output: [][]string{{"a", "b", "c"}},
	}, {
		Name:   "Semicolon",
		Input:  "a;b;c\n",
		Output: [][]string{{"a", "b", "c"}},
		Comma:  ';',
	}, {
		Name: "MultiLine",
		Input: `"two
line","one line","three
line
field"`,
		Output: [][]string{{"two\nline", "one line", "three\nline\nfield"}},
	}, {
		Name:  "BlankLine",
		Input: "a,b,c\n\nd,e,f\n\n",
		Output: [][]string{
			{"a", "b", "c"},
			{"d", "e", "f"},
		},
	}, {
		Name:  "BlankLineFieldCount",
		Input: "a,b,c\n\nd,e,f\n\n",
		Output: [][]string{
			{"a", "b", "c"},
			{"d", "e", "f"},
		},
		UseFieldsPerRecord: true,
		FieldsPerRecord:    0,
	}, {
		Name:             "TrimSpace",
		Input:            " a,  b,   c\n",
		Output:           [][]string{{"a", "b", "c"}},
		TrimLeadingSpace: true,
	}, {
		Name:   "LeadingSpace",
		Input:  " a,  b,   c\n",
		Output: [][]string{{" a", "  b", "   c"}},
	}, {
		Name:    "Comment",
		Input:   "#1,2,3\na,b,c\n#comment",
		Output:  [][]string{{"a", "b", "c"}},
		Comment: '#',
	}, {
		Name:   "NoComment",
		Input:  "#1,2,3\na,b,c",
		Output: [][]string{{"#1", "2", "3"}, {"a", "b", "c"}},
	}, {
		Name:       "LazyQuotes",
		Input:      `a "word","1"2",a","b`,
		Output:     [][]string{{`a "word"`, `1"2`, `a"`, `b`}},
		LazyQuotes: true,
	}, {
		Name:       "BareQuotes",
		Input:      `a "word","1"2",a"`,
		Output:     [][]string{{`a "word"`, `1"2`, `a"`}},
		LazyQuotes: true,
	}, {
		Name:       "BareDoubleQuotes",
		Input:      `a""b,c`,
		Output:     [][]string{{`a""b`, `c`}},
		LazyQuotes: true,
	}, {
		Name:  "BadDoubleQuotes",
		Input: `a""b,c`,
		Error: &csv.ParseError{StartLine: 1, Line: 1, Column: 1, Err: csv.ErrBareQuote},
	}, {
		Name:             "TrimQuote",
		Input:            ` "a"," b",c`,
		Output:           [][]string{{"a", " b", "c"}},
		TrimLeadingSpace: true,
	}, {
		Name:  "BadBareQuote",
		Input: `a "word","b"`,
		Error: &csv.ParseError{StartLine: 1, Line: 1, Column: 2, Err: csv.ErrBareQuote},
	}, {
		Name:  "BadTrailingQuote",
		Input: `"a word",b"`,
		Error: &csv.ParseError{StartLine: 1, Line: 1, Column: 10, Err: csv.ErrBareQuote},
	}, {
		Name:  "ExtraneousQuote",
		Input: `"a "word","b"`,
		Error: &csv.ParseError{StartLine: 1, Line: 1, Column: 3, Err: csv.ErrQuote},
	}, {
		Name:               "BadFieldCount",
		Input:              "a,b,c\nd,e",
		Error:              &csv.ParseError{StartLine: 2, Line: 2, Err: csv.ErrFieldCount},
		UseFieldsPerRecord: true,
		FieldsPerRecord:    0,
	}, {
		Name:               "BadFieldCount1",
		Input:              `a,b,c`,
		Error:              &csv.ParseError{StartLine: 1, Line: 1, Err: csv.ErrFieldCount},
		UseFieldsPerRecord: true,
		FieldsPerRecord:    2,
	}, {
		Name:   "FieldCount",
		Input:  "a,b,c\nd,e",
		Output: [][]string{{"a", "b", "c"}, {"d", "e"}},
	}, {
		Name:   "TrailingCommaEOF",
		Input:  "a,b,c,",
		Output: [][]string{{"a", "b", "c", ""}},
	}, {
		Name:   "TrailingCommaEOL",
		Input:  "a,b,c,\n",
		Output: [][]string{{"a", "b", "c", ""}},
	}, {
		Name:             "TrailingCommaSpaceEOF",
		Input:            "a,b,c, ",
		Output:           [][]string{{"a", "b", "c", ""}},
		TrimLeadingSpace: true,
	}, {
		Name:             "TrailingCommaSpaceEOL",
		Input:            "a,b,c, \n",
		Output:           [][]string{{"a", "b", "c", ""}},
		TrimLeadingSpace: true,
	}, {
		Name:             "TrailingCommaLine3",
		Input:            "a,b,c\nd,e,f\ng,hi,",
		Output:           [][]string{{"a", "b", "c"}, {"d", "e", "f"}, {"g", "hi", ""}},
		TrimLeadingSpace: true,
	}, {
		Name:   "NotTrailingComma3",
		Input:  "a,b,c, \n",
		Output: [][]string{{"a", "b", "c", " "}},
	}, {
		Name: "CommaFieldTest",
		Input: `x,y,z,w
x,y,z,
x,y,,
x,,,
,,,
"x","y","z","w"
"x","y","z",""
"x","y","",""
"x","","",""
"","","",""
`,
		Output: [][]string{
			{"x", "y", "z", "w"},
			{"x", "y", "z", ""},
			{"x", "y", "", ""},
			{"x", "", "", ""},
			{"", "", "", ""},
			{"x", "y", "z", "w"},
			{"x", "y", "z", ""},
			{"x", "y", "", ""},
			{"x", "", "", ""},
			{"", "", "", ""},
		},
	}, {
		Name:  "TrailingCommaIneffective1",
		Input: "a,b,\nc,d,e",
		Output: [][]string{
			{"a", "b", ""},
			{"c", "d", "e"},
		},
		TrimLeadingSpace: true,
	}, {
		Name:  "ReadAllReuseRecord",
		Input: "a,b\nc,d",
		Output: [][]string{
			{"a", "b"},
			{"c", "d"},
		},
		ReuseRecord: true,
	}, {
		Name:  "StartLine1", // Issue 19019
		Input: "a,\"b\nc\"d,e",
		Error: &csv.ParseError{StartLine: 1, Line: 2, Column: 1, Err: csv.ErrQuote},
	}, {
		Name:  "StartLine2",
		Input: "a,b\n\"d\n\n,e",
		Error: &csv.ParseError{StartLine: 2, Line: 5, Column: 0, Err: csv.ErrQuote},
	}, {
		Name:  "CRLFInQuotedField", // Issue 21201
		Input: "A,\"Hello\r\nHi\",B\r\n",
		Output: [][]string{
			{"A", "Hello\nHi", "B"},
		},
	}, {
		Name:   "BinaryBlobField", // Issue 19410
		Input:  "x09\x41\xb4\x1c,aktau",
		Output: [][]string{{"x09A\xb4\x1c", "aktau"}},
	}, {
		Name:   "TrailingCR",
		Input:  "field1,field2\r",
		Output: [][]string{{"field1", "field2"}},
	}, {
		Name:   "QuotedTrailingCR",
		Input:  "\"field\"\r",
		Output: [][]string{{"field"}},
	}, {
		Name:  "QuotedTrailingCRCR",
		Input: "\"field\"\r\r",
		Error: &csv.ParseError{StartLine: 1, Line: 1, Column: 6, Err: csv.ErrQuote},
	}, {
		Name:   "FieldCR",
		Input:  "field\rfield\r",
		Output: [][]string{{"field\rfield"}},
	}, {
		Name:   "FieldCRCR",
		Input:  "field\r\rfield\r\r",
		Output: [][]string{{"field\r\rfield\r"}},
	}, {
		Name:   "FieldCRCRLF",
		Input:  "field\r\r\nfield\r\r\n",
		Output: [][]string{{"field\r"}, {"field\r"}},
	}, {
		Name:   "FieldCRCRLFCR",
		Input:  "field\r\r\n\rfield\r\r\n\r",
		Output: [][]string{{"field\r"}, {"\rfield\r"}},
	}, {
		Name:   "FieldCRCRLFCRCR",
		Input:  "field\r\r\n\r\rfield\r\r\n\r\r",
		Output: [][]string{{"field\r"}, {"\r\rfield\r"}, {"\r"}},
	}, {
		Name:  "MultiFieldCRCRLFCRCR",
		Input: "field1,field2\r\r\n\r\rfield1,field2\r\r\n\r\r,",
		Output: [][]string{
			{"field1", "field2\r"},
			{"\r\rfield1", "field2\r"},
			{"\r\r", ""},
		},
	}, {
		Name:             "NonASCIICommaAndComment",
		Input:            "a£b,c£ \td,e\n€ comment\n",
		Output:           [][]string{{"a", "b,c", "d,e"}},
		TrimLeadingSpace: true,
		Comma:            '£',
		Comment:          '€',
	}, {
		Name:    "NonASCIICommaAndCommentWithQuotes",
		Input:   "a€\"  b,\"€ c\nλ comment\n",
		Output:  [][]string{{"a", "  b,", " c"}},
		Comma:   '€',
		Comment: 'λ',
	}, {
		// λ and θ start with the same byte.
		// This tests that the parser doesn't confuse such characters.
		Name:    "NonASCIICommaConfusion",
		Input:   "\"abθcd\"λefθgh",
		Output:  [][]string{{"abθcd", "efθgh"}},
		Comma:   'λ',
		Comment: '€',
	}, {
		Name:    "NonASCIICommentConfusion",
		Input:   "λ\nλ\nθ\nλ\n",
		Output:  [][]string{{"λ"}, {"λ"}, {"λ"}},
		Comment: 'θ',
	}, {
		Name:   "QuotedFieldMultipleLF",
		Input:  "\"\n\n\n\n\"",
		Output: [][]string{{"\n\n\n\n"}},
	}, {
		Name:  "MultipleCRLF",
		Input: "\r\n\r\n\r\n\r\n",
	}, {
		// The implementation may read each line in several chunks if it doesn't fit entirely
		// in the read buffer, so we should test the code to handle that condition.
		Name:    "HugeLines",
		Input:   strings.Repeat("#ignore\n", 10000) + strings.Repeat("@", 5000) + "," + strings.Repeat("*", 5000),
		Output:  [][]string{{strings.Repeat("@", 5000), strings.Repeat("*", 5000)}},
		Comment: '#',
	}, {
		Name:  "QuoteWithTrailingCRLF",
		Input: "\"foo\"bar\"\r\n",
		Error: &csv.ParseError{StartLine: 1, Line: 1, Column: 4, Err: csv.ErrQuote},
	}, {
		Name:       "LazyQuoteWithTrailingCRLF",
		Input:      "\"foo\"bar\"\r\n",
		Output:     [][]string{{`foo"bar`}},
		LazyQuotes: true,
	}, {
		Name:   "DoubleQuoteWithTrailingCRLF",
		Input:  "\"foo\"\"bar\"\r\n",
		Output: [][]string{{`foo"bar`}},
	}, {
		Name:   "EvenQuotes",
		Input:  `""""""""`,
		Output: [][]string{{`"""`}},
	}, {
		Name:  "OddQuotes",
		Input: `"""""""`,
		Error: &csv.ParseError{StartLine: 1, Line: 1, Column: 7, Err: csv.ErrQuote},
	}, {
		Name:       "LazyOddQuotes",
		Input:      `"""""""`,
		Output:     [][]string{{`"""`}},
		LazyQuotes: true,
	}, {
		Name:  "BadComma1",
		Comma: '\n',
		Error: errInvalidDelim,
	}, {
		Name:  "BadComma2",
		Comma: '\r',
		Error: errInvalidDelim,
	}, {
		Name:  "BadComma3",
		Comma: '"',
		Error: errInvalidDelim,
	}, {
		Name:  "BadComma4",
		Comma: utf8.RuneError,
		Error: errInvalidDelim,
	}, {
		Name:    "BadComment1",
		Comment: '\n',
		Error:   errInvalidDelim,
	}, {
		Name:    "BadComment2",
		Comment: '\r',
		Error:   errInvalidDelim,
	}, {
		Name:    "BadComment3",
		Comment: utf8.RuneError,
		Error:   errInvalidDelim,
	}, {
		Name:    "BadCommaComment",
		Comma:   'X',
		Comment: 'X',
		Error:   errInvalidDelim,
	}, {
		Name:   "Unicode",
		Input:  "AB,C€,b,c\n",
		Output: [][]string{{"AB", "C€", "b", "c"}},
	}}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			r := NewReader(strings.NewReader(tt.Input))

			if tt.Comma != 0 {
				r.Comma = tt.Comma
			}
			r.Comment = tt.Comment
			if tt.UseFieldsPerRecord {
				r.FieldsPerRecord = tt.FieldsPerRecord
			} else {
				r.FieldsPerRecord = -1
			}
			r.LazyQuotes = tt.LazyQuotes
			r.TrimLeadingSpace = tt.TrimLeadingSpace
			r.ReuseRecord = tt.ReuseRecord

			out := [][]string{}
			var err error
			out, err = r.ReadAll()
			if !reflect.DeepEqual(err, tt.Error) {
				t.Errorf("ReadAll() error:\ngot  %v\nwant %v", err, tt.Error)
			} else if !reflect.DeepEqual(out, tt.Output) {
				t.Errorf("ReadAll() output:\ngot  %q\nwant %q", out, tt.Output)
			}
		})
	}
}

func TestLosAngelesParkingCitations(t *testing.T) {

	t.Run("single-line", func(t *testing.T) {
		const test = `4277258042,2016-02-09T00:00:00.000,459,,,NJ,,,KW,CM,RD,"3772 MARTIN LUTHER KING, JR BLVD W",00500,55,80.69B,NO PARKING,73,99999,99999,,,
`
		compareAgainstEncodingCsv(t, []byte(test))
	})

	t.Run("multiple-lines", func(t *testing.T) {
		const test = `1103341116,2015-12-21T00:00:00.000,1251,,,CA,200304,,HOND,PA,GY,13147 WELBY WAY,01521,1,4000A1,NO EVIDENCE OF REG,50,99999,99999,,,
1103700150,2015-12-21T00:00:00.000,1435,,,CA,201512,,GMC,VN,WH,525 S MAIN ST,1C51,1,4000A1,NO EVIDENCE OF REG,50,99999,99999,,,
1104803000,2015-12-21T00:00:00.000,2055,,,CA,201503,,NISS,PA,BK,200 WORLD WAY,2R2,2,8939,WHITE CURB,58,6439997.9,1802686.4,,,
1104820732,2015-12-26T00:00:00.000,1515,,,CA,,,ACUR,PA,WH,100 WORLD WAY,2F11,2,000,17104h,,6440041.1,1802686.2,,,
1105461453,2015-09-15T00:00:00.000,115,,,CA,200316,,CHEV,PA,BK,GEORGIA ST/OLYMPIC,1FB70,1,8069A,NO STOPPING/STANDING,93,99999,99999,,,
1106226590,2015-09-15T00:00:00.000,19,,,CA,201507,,CHEV,VN,GY,SAN PEDRO S/O BOYD,1A35W,1,4000A1,NO EVIDENCE OF REG,50,99999,99999,,,
1106500452,2015-12-17T00:00:00.000,1710,,,CA,201605,,MAZD,PA,BL,SUNSET/ALVARADO,00217,1,8070,PARK IN GRID LOCK ZN,163,99999,99999,,,
1106500463,2015-12-17T00:00:00.000,1710,,,CA,201602,,TOYO,PA,BK,SUNSET/ALVARADO,00217,1,8070,PARK IN GRID LOCK ZN,163,99999,99999,,,
1106506402,2015-12-22T00:00:00.000,945,,,CA,201605,,CHEV,PA,BR,721 S WESTLAKE,2A75,1,8069AA,NO STOP/STAND AM,93,99999,99999,,,
`
		compareAgainstEncodingCsv(t, []byte(test))
	})

	t.Run("multiple-lines-with-header", func(t *testing.T) {
		const test = `Ticket number,Issue Date,Issue time,Meter Id,Marked Time,RP State Plate,Plate Expiry Date,VIN,Make,Body Style,Color,Location,Route,Agency,Violation code,Violation Description,Fine amount,Latitude,Longitude,Agency Description,Color Description,Body Style Description
1103341116,2015-12-21T00:00:00.000,1251,,,CA,200304,,HOND,PA,GY,13147 WELBY WAY,01521,1,4000A1,NO EVIDENCE OF REG,50,99999,99999,,,
1103700150,2015-12-21T00:00:00.000,1435,,,CA,201512,,GMC,VN,WH,525 S MAIN ST,1C51,1,4000A1,NO EVIDENCE OF REG,50,99999,99999,,,
1104803000,2015-12-21T00:00:00.000,2055,,,CA,201503,,NISS,PA,BK,200 WORLD WAY,2R2,2,8939,WHITE CURB,58,6439997.9,1802686.4,,,
1104820732,2015-12-26T00:00:00.000,1515,,,CA,,,ACUR,PA,WH,100 WORLD WAY,2F11,2,000,17104h,,6440041.1,1802686.2,,,
1105461453,2015-09-15T00:00:00.000,115,,,CA,200316,,CHEV,PA,BK,GEORGIA ST/OLYMPIC,1FB70,1,8069A,NO STOPPING/STANDING,93,99999,99999,,,
1106226590,2015-09-15T00:00:00.000,19,,,CA,201507,,CHEV,VN,GY,SAN PEDRO S/O BOYD,1A35W,1,4000A1,NO EVIDENCE OF REG,50,99999,99999,,,
1106500452,2015-12-17T00:00:00.000,1710,,,CA,201605,,MAZD,PA,BL,SUNSET/ALVARADO,00217,1,8070,PARK IN GRID LOCK ZN,163,99999,99999,,,
1106500463,2015-12-17T00:00:00.000,1710,,,CA,201602,,TOYO,PA,BK,SUNSET/ALVARADO,00217,1,8070,PARK IN GRID LOCK ZN,163,99999,99999,,,
1106506402,2015-12-22T00:00:00.000,945,,,CA,201605,,CHEV,PA,BR,721 S WESTLAKE,2A75,1,8069AA,NO STOP/STAND AM,93,99999,99999,,,
`
		compareAgainstEncodingCsv(t, []byte(test))
	})

	t.Run("quoted-lines", func(t *testing.T) {
		const test = `4272958045,2015-12-31T00:00:00.000,847,,,CA,201503,,JAGU,PA,BL,"3749 MARTIN LUTHER KING, JR BLVD",57B,56,5204A-,DISPLAY OF TABS,25,6459025.9,1827359.3,,,
4248811976,2015-01-12T00:00:00.000,541,,,CA,201507,,CHEV,PA,WT,"107 S,ARBOLES COVRT",00503,56,22500E,BLOCKING DRIVEWAY,68,6475910.9,1729065.4,,,
4275646756,2016-01-19T00:00:00.000,1037,,,NY,,,CADI,PA,BK,"641, CALHOUN AVE",378R1,53,80.69BS,NO PARK/STREET CLEAN,73,99999,99999,,,
4276086533,2016-02-04T00:00:00.000,1121,,1013,CA,,,VOLK,PA,SL,"31,00 7TH ST W",00463,54,80.69C,PARKED OVER TIME LIM,58,99999,99999,,,
4277212796,2016-02-17T00:00:00.000,1602,,1140,GA,201610,,CHEV,PA,BK,"130, ELECTRIC AVE",908R,51,80.69C,PARKED OVER TIME LIM,58,99999,99999,,,
4277882641,2016-02-23T00:00:00.000,719,,,CA,6,,HOND,PA,GN,"18,2 MAIN ST S",00656,56,80.69AA+,NO STOP/STAND,93,99999,99999,,,
4276685420,2016-02-25T00:00:00.000,812,,,FL,,,CHRY,PA,BK,"3281, PERLITA AVE",00674,56,80.69BS,NO PARK/STREET CLEAN,73,99999,99999,,,
4277393536,2016-03-08T00:00:00.000,2247,,,CA,201603,,MITS,PA,MR,"1579 KING, JR BLVD",00500,55,22500E,BLOCKING DRIVEWAY,68,99999,99999,,,
4280358482,2016-04-07T00:00:00.000,857,,,CA,201606,,UNK,MH,WT,",1931 WEST AVENUE 30",00673,56,80.69BS,NO PARK/STREET CLEAN,73,99999,99999,,,
4281118855,2016-04-17T00:00:00.000,1544,",5",,CA,201703,,FORD,VN,GN,330 SOUTH HAMEL ROAD,00401,54,80.58L,PREFERENTIAL PARKING,68,6446091.5,1849240.9,,,
4251090233,2015-01-14T00:00:00.000,1138,,,CA,201504,,FORD,PU,BL,"772, LANKERSHIM BLVD",378R1,53,80.69BS,NO PARK/STREET CLEAN,73,99999,99999,,,
4284911094,2016-06-21T00:00:00.000,1520,,,CA,6,,KIA,PA,BK,"3171, OLYMPIC BLVD",00456,54,80.70,NO STOPPING/ANTI-GRI,163,99999,99999,,,
4277258042,2016-02-09T00:00:00.000,459,,,NJ,,,KW,CM,RD,"3772 MARTIN LUTHER KING, JR BLVD W",00500,55,80.69B,NO PARKING,73,99999,99999,,,
`
		compareAgainstEncodingCsv(t, []byte(test))
	})

	t.Run("all-and-long", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping... too long")
		}

		var buf []byte
		var err error
		if buf, err = ioutil.ReadFile("testdata/parking-citations.csv"); err != nil {
			t.Skip("skipping... test dataset not found")
		}

		buf = bytes.ReplaceAll(buf, []byte{0x0d}, []byte{})
		lines := bytes.Split(buf, []byte("\n"))
		lines = lines[1:]

		for len(lines) > 0 {
			ln := 10000
			if len(lines) < ln {
				ln = len(lines)
			}

			test := bytes.Join(lines[:ln], []byte{0x0a})
			compareAgainstEncodingCsv(t, test)

			lines = lines[ln:]

			runtime.GC()
		}
	})
}

func TestSimdCsv(t *testing.T) {
	t.Run("parking-citations-100K", func(t *testing.T) {
		testSimdCsv(t, "testdata/parking-citations-100K.csv")
	})
	t.Run("worldcitiespop-100K", func(t *testing.T) {
		testSimdCsv(t, "testdata/worldcitiespop-100K.csv")
	})
	t.Run("nyc-taxi-100K", func(t *testing.T) {
		testSimdCsv(t, "testdata/nyc-taxi-data-100K.csv")
	})
}

func testSimdCsv(t *testing.T, filename string) {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Errorf("%v", err)
	}
	compareAgainstEncodingCsv(t, buf)
}

func compareAgainstEncodingCsv(t *testing.T, test []byte) {

	records, err := encodingCsv(test)
	if err != nil {
		log.Fatalf("%v", err)
	}
	r := NewReader(bytes.NewReader(test))
	simdrecords, err := r.ReadAll()
	if err != nil {
		log.Fatalf("%v", err)
	}

	if !reflect.DeepEqual(simdrecords, records) {
		t.Errorf("compareAgainstEncodingCsv: got: %v want: %v", len(simdrecords), len(records))
	}
}

// filter out commented rows before returning to client
func testIgnoreCommentedLines(t *testing.T, csvData []byte) {

	const comment = '#'

	simdr := NewReader(bytes.NewReader(csvData))
	simdr.FieldsPerRecord = -1
	simdrecords, err := simdr.ReadAll()
	if err != nil {
		log.Fatalf("%v", err)
	}
	filterOutComments(&simdrecords, comment)

	r := csv.NewReader(bytes.NewReader(csvData))
	r.Comment = comment
	records, err := r.ReadAll()
	if err != nil {
		log.Fatalf("%v", err)
	}

	if !reflect.DeepEqual(simdrecords, records) {
		t.Errorf("testIgnoreCommentedLines: got: %v want: %v", simdrecords, records)
	}
}

func TestIgnoreCommentedLines(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		testIgnoreCommentedLines(t, []byte("a,b,c\n#hello,good,bye\nd,e,f\n\n"))
	})
	t.Run("first", func(t *testing.T) {
		testIgnoreCommentedLines(t, []byte("#a,b,c\nhello,good,bye\nd,e,f\n\n"))
	})
	t.Run("last", func(t *testing.T) {
		testIgnoreCommentedLines(t, []byte("a,b,c\nd,e,f\n#IGNORED\n"))
	})
	t.Run("multiple", func(t *testing.T) {
		testIgnoreCommentedLines(t, []byte("a,b,c\n#A,B,C\nd,e,f\n#g,h,i\n"))
	})
}

func testFieldsPerRecord(t *testing.T, csvData []byte, fieldsPerRecord int64) {

	simdr := NewReader(bytes.NewReader(csvData))
	simdr.FieldsPerRecord = int(fieldsPerRecord)
	simdrecords, errSimd := simdr.ReadAll()

	r := csv.NewReader(bytes.NewReader(csvData))
	r.FieldsPerRecord = int(fieldsPerRecord)
	records, err := r.ReadAll()

	// are both returning errors, then this test is a pass
	if errSimd != nil && err != nil {
		return
	}

	if !reflect.DeepEqual(simdrecords, records) {
		t.Errorf("TestFieldsPerRecord: got: %v want: %v", simdrecords, records)
	}
}

func TestEnsureFieldsPerRecord(t *testing.T) {

	t.Run("match", func(t *testing.T) {
		testFieldsPerRecord(t, []byte("a,b,c\nd,e,f\ng,h,i\n"), 3)
	})
	t.Run("fail", func(t *testing.T) {
		testFieldsPerRecord(t, []byte("a,b,c\nd,e,f\ng,h,i\n"), 4)
	})
	t.Run("variable", func(t *testing.T) {
		testFieldsPerRecord(t, []byte("a,b,c\nd,e\ng\n"), -1)
	})
	t.Run("auto-pass", func(t *testing.T) {
		testFieldsPerRecord(t, []byte("a,b,c\nd,e,f\ng,h,i\n"), 0)
	})
	t.Run("auto-fail", func(t *testing.T) {
		testFieldsPerRecord(t, []byte("a,b,c\nd,e\ng,h\n"), 0)
	})
}

func testTrimLeadingSpace(t *testing.T, csvData []byte) {

	simdr := NewReader(bytes.NewReader(csvData))
	simdrecords, err := simdr.ReadAll()
	if err != nil {
		log.Fatalf("%v", err)
	}
	trimLeadingSpace(&simdrecords)

	r := csv.NewReader(bytes.NewReader(csvData))
	r.TrimLeadingSpace = true
	records, err := r.ReadAll()
	if err != nil {
		log.Fatalf("%v", err)
	}

	if !reflect.DeepEqual(simdrecords, records) {
		t.Errorf("testTrimLeadingSpace: got: %v want: %v", simdrecords, records)
	}
}

func TestTrimLeadingSpace(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		testTrimLeadingSpace(t, []byte("a,b,c\n d, e, f\n"))
	})
	t.Run("tabs", func(t *testing.T) {
		testTrimLeadingSpace(t, []byte("\tg,h,i\n"))
	})
	t.Run("unicode", func(t *testing.T) {
		testTrimLeadingSpace(t, []byte("j,"+string('\u00A0')+"k,l\n"))
	})
}

func TestExample(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping example")
	}

	// Example based on https://play.golang.org/p/XxthE8qqZtZ

	// NB You are free to change this example, just make sure it does not extend beyond 128 bytes !
	instr := `first_name,last_name,username
"Rob","Pike",rob
Ken,Thompson,ken
"Robert","Griesemer","gri"
`

	// Merge in some special behaviour/corner cases
	instr = strings.Replace(instr, "\n", "\r\n", 1)                     // change regular newline into carriage return and newline pair
	instr = strings.Replace(instr, `"Rob"`, `"Ro""b"`, 1)               // pair of double quotes in quoted field that act as an escaped quote
	instr = strings.Replace(instr, `"Pike"`, `"Pi,ke"`, 1)              // separator character in quoted field that shoule be disabled
	instr = strings.Replace(instr, `"Robert"`, `"Rob`+"\r\n"+`ert"`, 1) // carriage return in quoted field followed by newline --> treated as newline
	instr = strings.Replace(instr, `"Griesemer"`, "Gries\remer", 1)     // carriage return in quoted field not followed by newline  --> not treated as newline

	buf := make([]byte, 128)
	copy(buf, instr)

	out := bytes.NewBufferString("")

	fmt.Fprintln(out, hex.Dump(buf))

	//
	// Stage 1: preprocessing
	//

	fmt.Fprintf(out, "         input: %s", string(bytes.ReplaceAll(bytes.ReplaceAll(buf[:64], []byte{0xd}, []byte{0x20}), []byte{0xa}, []byte{0x20})))
	fmt.Fprintf(out, "·%s\n", string(bytes.ReplaceAll(bytes.ReplaceAll(buf[64:], []byte{0xd}, []byte{0x20}), []byte{0xa}, []byte{0x20})))

	separatorMasksIn := getBitMasks(buf, byte(','))
	quoteMasksIn := getBitMasks(buf, byte('"'))
	carriageReturnMasksIn := getBitMasks(buf, byte('\r'))
	newlineMasksIn := getBitMasks(buf, byte('\n'))

	input1 := stage1Input{quoteMasksIn[0], separatorMasksIn[0], carriageReturnMasksIn[0], quoteMasksIn[1], 0, newlineMasksIn[0], newlineMasksIn[1]}
	output1_0 := stage1Output{}
	preprocessMasks(&input1, &output1_0)

	input1 = stage1Input{input1.quoteMaskInNext, separatorMasksIn[1], carriageReturnMasksIn[1], 0, input1.quoted, newlineMasksIn[1], 0}
	output1_1 := stage1Output{}
	preprocessMasks(&input1, &output1_1)

	d := strings.Split(diffBitmask(
		fmt.Sprintf("%064b·%064b", bits.Reverse64(quoteMasksIn[0]), bits.Reverse64(quoteMasksIn[1])),
		fmt.Sprintf("%064b·%064b", bits.Reverse64(output1_0.quoteMaskOut), bits.Reverse64(output1_1.quoteMaskOut))), "\n")

	fmt.Fprintf(out, "%s\n%s\n%s\n",
		fmt.Sprintf("     quote-in : %s", d[0]),
		fmt.Sprintf("     quote-out: %s", d[1]),
		fmt.Sprintf("                %s", d[2]))

	d = strings.Split(diffBitmask(
		fmt.Sprintf("%064b·%064b", bits.Reverse64(separatorMasksIn[0]), bits.Reverse64(separatorMasksIn[1])),
		fmt.Sprintf("%064b·%064b", bits.Reverse64(output1_0.separatorMaskOut), bits.Reverse64(output1_1.separatorMaskOut))), "\n")

	fmt.Fprintf(out, "%s\n%s\n%s\n",
		fmt.Sprintf(" separator-in : %s", d[0]),
		fmt.Sprintf(" separator-out: %s", d[1]),
		fmt.Sprintf("                %s", d[2]))

	d = strings.Split(diffBitmask(
		fmt.Sprintf("%064b·%064b", bits.Reverse64(carriageReturnMasksIn[0]), bits.Reverse64(carriageReturnMasksIn[1])),
		fmt.Sprintf("%064b·%064b", bits.Reverse64(output1_0.carriageReturnMaskOut), bits.Reverse64(output1_1.carriageReturnMaskOut))), "\n")

	fmt.Fprintf(out, "%s\n%s\n%s\n",
		fmt.Sprintf("  carriage-in : %s", d[0]),
		fmt.Sprintf("  carriage-out: %s", d[1]),
		fmt.Sprintf("                %s", d[2]))

	//
	// Stage 2: parsing
	//

	input2 := newInputStage2()
	input2.quoteMask = output1_0.quoteMaskOut
	input2.separatorMask = output1_0.separatorMaskOut
	input2.delimiterMask = newlineMasksIn[0] | output1_0.carriageReturnMaskOut

	output2 := outputStage2{}
	output2.columns = &[128]uint64{}
	output2.rows = &[128]uint64{}

	stage2ParseMasks(&input2, 0, &output2)

	input2.quoteMask = output1_1.quoteMaskOut
	input2.separatorMask = output1_1.separatorMaskOut
	input2.delimiterMask = newlineMasksIn[1] | output1_1.carriageReturnMaskOut

	fmt.Fprintln(out)
	fmt.Fprintf(out, "                %s\n", strings.Repeat(" -", 64))

	fmt.Fprintf(out, "%s\n", fmt.Sprintf("        quotes: %064b·%064b", bits.Reverse64(output1_0.quoteMaskOut), bits.Reverse64(output1_1.quoteMaskOut)))
	fmt.Fprintf(out, "%s\n", fmt.Sprintf("     delimiter: %064b·%064b", bits.Reverse64(newlineMasksIn[0]|output1_0.carriageReturnMaskOut), bits.Reverse64(input2.delimiterMask)))
	fmt.Fprintf(out, "%s\n", fmt.Sprintf("     separator: %064b·%064b", bits.Reverse64(output1_0.separatorMaskOut), bits.Reverse64(output1_1.separatorMaskOut)))
	fmt.Fprintf(out, "         input: %s", string(bytes.ReplaceAll(bytes.ReplaceAll(buf[:64], []byte{0xd}, []byte{0x20}), []byte{0xa}, []byte{0x20})))
	fmt.Fprintf(out, "·%s\n", string(bytes.ReplaceAll(bytes.ReplaceAll(buf[64:], []byte{0xd}, []byte{0x20}), []byte{0xa}, []byte{0x20})))

	stage2ParseMasks(&input2, 64, &output2)

	splitAt64 := func(str string) string {
		if len(str) >= 64 {
			str = str[:64] + "·" + str[64:]
		}
		return strings.ReplaceAll(strings.ReplaceAll(str, "\r", " "), "\n", " ")
	}

	for line := 0; line < output2.line; line += 2 {
		start, size := output2.rows[line], output2.rows[line+1]

		outline := bytes.Repeat([]byte(" "), 128)

		for column := uint64(0); column < size; column++ {
			copy(outline[output2.columns[(start+column)*2]:], buf[output2.columns[(start+column)*2]:output2.columns[(start+column)*2]+output2.columns[(start+column)*2+1]])
		}
		fmt.Fprintf(out, "        row[%d]: %s\n", line/2, splitAt64(string(outline)))
	}

	fmt.Fprintln(out)
	fmt.Fprintf(out, "                %s\n", strings.Repeat(" -", 64))

	//
	// Post processing
	//

	for line := 0; line < output2.line; line += 2 {
		start, size := output2.rows[line], output2.rows[line+1]

		outline := bytes.Repeat([]byte(" "), 128)

		for column := uint64(0); column < size; column++ {
			v := string(buf[output2.columns[(start+column)*2] : output2.columns[(start+column)*2]+output2.columns[(start+column)*2+1]])
			if strings.Contains(v, `""`) || strings.Contains(v, "\r\n") {
				v = strings.ReplaceAll(v, "\"\"", "\"")
				v = strings.ReplaceAll(v, "\r\n", "\n")
				copy(outline[output2.columns[(start+column)*2]:], []byte(v))
			}
		}
		if len(strings.TrimSpace(string(outline))) > 0 {
			fmt.Fprintf(out, "        row[%d]: %s\n", line/2, splitAt64(string(outline)))
		}
	}

	fmt.Println(out.String())
}

func BenchmarkSimdCsv(b *testing.B) {
	b.Run("parking-citations-100K", func(b *testing.B) {
		benchmarkSimdCsv(b, "testdata/parking-citations-100K.csv")
	})
	b.Run("worldcitiespop-100K", func(b *testing.B) {
		benchmarkSimdCsv(b, "testdata/worldcitiespop-100K.csv")
	})
	b.Run("nyc-taxi-data-100K", func(b *testing.B) {
		benchmarkSimdCsv(b, "testdata/nyc-taxi-data-100K.csv")
	})
}

func benchmarkSimdCsv(b *testing.B, file string) {

	buf, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}

	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := NewReader(bytes.NewReader(buf))
		_, err := r.ReadAll()
		if err != nil {
			log.Fatalf("%v", err)
		}
	}
}

func BenchmarkEncodingCsv(b *testing.B) {
	b.Run("parking-citations-100K", func(b *testing.B) {
		benchmarkEncodingCsv(b, "testdata/parking-citations-100K.csv")
	})
	b.Run("worldcitiespop-100K", func(b *testing.B) {
		benchmarkEncodingCsv(b, "testdata/worldcitiespop-100K.csv")
	})
	b.Run("nyc-taxi-data-100K", func(b *testing.B) {
		benchmarkEncodingCsv(b, "testdata/nyc-taxi-data-100K.csv")
	})
}

func benchmarkEncodingCsv(b *testing.B, file string) {

	data, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := csv.NewReader(bytes.NewReader(data))
		_, err := r.ReadAll()
		if err != nil {
			log.Fatalf("%v", err)
		}
	}
}
