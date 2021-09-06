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
	"io/ioutil"
	"log"
	_ "math/bits"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func testStage2ParseUnquoted(t *testing.T, f func(input *inputStage2, offset uint64, output *outputStage2)) {

	const file = `a,bb,,ddd,eeee,,,hhhhh,,,,jjjjjj,,,,,ooooooo,,,,,,uuuuuuuu,,,,,
`
	//fmt.Println(hex.Dump([]byte(file)))

	output, _, _ := stage2Parse([]byte(file)[:64], '\n', ',', '"', f)
	expected := []uint64{0, 1, 2, 2, 5, 0, 6, 3, 10, 4, 15, 0, 16, 0, 17, 5, 23, 0, 24, 0, 25, 0, 26, 6, 33, 0, 34, 0, 35, 0, 36, 0, 37, 7, 45, 0, 46, 0, 47, 0, 48, 0, 49, 0, 50, 8, 59, 0, 60, 0, 61, 0, 62, 0, 0, 0}
	if !reflect.DeepEqual(output, expected) {
		t.Errorf("testStage2ParseUnquoted: got: %v want: %v", output, expected)
	}
}

func TestStage2ParseUnquoted(t *testing.T) {
	t.Run("go", func(t *testing.T) {
		testStage2ParseUnquoted(t, stage2ParseMasks)
	})
	t.Run("avx2", func(t *testing.T) {
		testStage2ParseUnquoted(t, stage2_parse_test)
	})
}

func testStage2ParseQuoted(t *testing.T, f func(input *inputStage2, offset uint64, output *outputStage2)) {

	const file = `A,"A",BB,,"DDD","EEEE","",,HHHHH,,,,JJJJJJ,,,,,OOOOOOO,,,,,,UUU
`
	//fmt.Println(hex.Dump([]byte(file)))

	output, _, _ := stage2Parse([]byte(file)[:64], '\n', ',', '"', f)
	expected := []uint64{0, 1, 3, 1, 6, 2, 9, 0, 11, 3, 17, 4, 24, 0, 26, 0, 27, 5, 33, 0, 34, 0, 35, 0, 36, 6, 43, 0, 44, 0, 45, 0, 46, 0, 47, 7, 55, 0, 56, 0, 57, 0, 58, 0, 59, 0, 60, 3}

	if !reflect.DeepEqual(output, expected) {
		t.Errorf("testStage2ParseQuoted: got: %v want: %v", output, expected)
	}
}

func TestStage2ParseQuoted(t *testing.T) {
	t.Run("go", func(t *testing.T) {
		testStage2ParseQuoted(t, stage2ParseMasks)
	})
	t.Run("avx2", func(t *testing.T) {
		testStage2ParseQuoted(t, stage2_parse_test)
	})
}

func testStage2ParseMultipleMasks(t *testing.T, f func(input *inputStage2, offset uint64, output *outputStage2)) {

	const file = `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,ccccccccccccccccccccccccccccccc,ddddddddddddddddddddddddddddddd
`
	//fmt.Println(hex.Dump([]byte(file)))

	output, _, _ := stage2Parse([]byte(file), '\n', ',', '"', f)
	expected := []uint64{0, 0x1f, 0x20, 0x1f, 0x40, 0x1f, 0x60, 0x1f}

	if !reflect.DeepEqual(output, expected) {
		t.Errorf("TestStage2ParseMultipleMasks: got: %v want: %v", output, expected)
	}
}

func TestStage2ParseMultipleMasks(t *testing.T) {
	t.Run("go", func(t *testing.T) {
		testStage2ParseMultipleMasks(t, stage2ParseMasks)
	})
	t.Run("avx2", func(t *testing.T) {
		testStage2ParseMultipleMasks(t, stage2_parse_test)
	})
}

func testStage2ParseMultipleRows(t *testing.T, f func(input *inputStage2, offset uint64, output *outputStage2)) {

	const file = `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,ccccccccccccccccccccccccccccccc,ddddddddddddddddddddddddddddddd
eeeeeeeeeeeeeeeeeeeeeeeeeeeeeee,fffffffffffffffffffffffffffffff,ggggggggggggggggggggggggggggggg,hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh
iiiiiiiiiiiiiiiiiiiiiiiiiiiiiii,jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj,kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk
lllllllllllllllllllllllllllllll
`
	//fmt.Println(hex.Dump([]byte(file)))

	columns, rows, _ := stage2Parse([]byte(file), '\n', ',', '"', f)
	expectedCols := []uint64{0, 0x1f, 0x20, 0x1f, 0x40, 0x1f, 0x60, 0x1f, 0x80, 0x1f, 0xa0, 0x1f, 0xc0, 0x1f, 0xe0, 0x1f, 0x100, 0x1f, 0x120, 0x1f, 0x140, 0x1f, 0x160, 0x1f}
	expectedRows := []uint64{0, 4, 4, 4, 8, 3, 11, 1}

	if !reflect.DeepEqual(columns, expectedCols) {
		t.Errorf("TestStage2ParseMultipleRows: got: %v want: %v", columns, expectedCols)
	}

	if !reflect.DeepEqual(rows, expectedRows) {
		t.Errorf("TestStage2ParseMultipleRows: got: %v want: %v", rows, expectedRows)
	}
}

func TestStage2ParseMultipleRows(t *testing.T) {
	t.Run("go", func(t *testing.T) {
		testStage2ParseMultipleRows(t, stage2ParseMasks)
	})
	t.Run("avx2", func(t *testing.T) {
		testStage2ParseMultipleRows(t, stage2_parse_test)
	})
}

func testBareQuoteInNonQuotedField(t *testing.T, f func(input *inputStage2, offset uint64, output *outputStage2)) {

	bareQuoteInNonQuotedFields := []struct {
		input    string
		expected uint64
	}{
		{` "aaaa","bbbb"`, 1},
		{`"aaaa", "bbbb"`, 8},
		{`"aaaa"
 "bbbb",`, 8},
	}

	for _, bareQuoteInNonQuotedField := range bareQuoteInNonQuotedFields {

		//r := csv.NewReader(strings.NewReader(bareQuoteInNonQuotedField.input))
		//
		//_, err := r.ReadAll()
		//if err == nil {
		//	log.Fatal("Expected error")
		//} else {
		//	fmt.Printf("%v\n", err)
		//}

		in := [64]byte{}
		copy(in[:], bareQuoteInNonQuotedField.input)
		_, _, errorOffset := stage2Parse(in[:], '\n', ',', '"', f)

		if errorOffset != bareQuoteInNonQuotedField.expected {
			t.Errorf("testBareQuoteInNonQuotedField: got: %d want: %d", errorOffset, bareQuoteInNonQuotedField.expected)
		}
	}
}

// Opening quote can only start after either , or delimiter
func TestBareQuoteInNonQuotedField(t *testing.T) {
	t.Run("go", func(t *testing.T) {
		testBareQuoteInNonQuotedField(t, stage2ParseMasks)
	})
	t.Run("avx2", func(t *testing.T) {
		testBareQuoteInNonQuotedField(t, stage2_parse_test)
	})
}

func testExtraneousOrMissingQuoteInQuotedField(t *testing.T, f func(input *inputStage2, offset uint64, output *outputStage2)) {

	extraneousOrMissingQuoteInQuotedFields := []struct {
		input    string
		expected uint64
	}{
		// 00000000  22 61 61 61 61 22 20 2c  62 62 62 62 00 00 00 00  |"aaaa" ,bbbb....|
		//                             ^^
		{`"aaaa" ,bbbb`, 7},
		//
		//
		// 00000000  22 61 61 61 61 22 20 0a  22 62 62 62 62 22 00 00  |"aaaa" ."bbbb"..|
		//                             ^^
		{`"aaaa" ` + `
"bbbb"`, 7},
		//
		//
		// 00000000  22 61 61 61 61 22 2c 22  62 62 62 62 22 20 00 00  |"aaaa","bbbb" ..|
		//                                                   ^^
		{`"aaaa","bbbb" `, 14},
		//
		//
		// Name:  "StartLine2"
		// 00000000  61 2c 62 0a 22 64 0a 0a  2c 65 00 00 00 00 00 00  |a,b."d..,e......|
		//                                          ^^
		{"a,b\n\"d\n\n,e", 64},
	}

	for _, extraneousOrMissingQuoteInQuotedField := range extraneousOrMissingQuoteInQuotedFields {

		//r := csv.NewReader(strings.NewReader(extraneousOrMissingQuoteInQuotedField.input))
		//
		//_, err := r.ReadAll()
		//if err == nil {
		//	log.Fatal("Expected error")
		//} else {
		//	fmt.Printf("%v\n", err)
		//}

		in := [64]byte{}
		copy(in[:], extraneousOrMissingQuoteInQuotedField.input)

		// TODO: Fix this hack: make sure we always end with a delimiter
		if in[len(extraneousOrMissingQuoteInQuotedField.input)-1] != '\n' {
			in[len(extraneousOrMissingQuoteInQuotedField.input)] = '\n'
		}

		_, _, errorOffset := stage2Parse(in[:], '\n', ',', '"', f)

		if errorOffset != extraneousOrMissingQuoteInQuotedField.expected {
			t.Errorf("TestExtraneousOrMissingQuoteInQuotedField: got: %d want: %d", errorOffset, extraneousOrMissingQuoteInQuotedField.expected)
		}
	}
}

// Closing quote needs to be followed immediately by either a , or delimiter
func TestExtraneousOrMissingQuoteInQuotedField(t *testing.T) {
	t.Run("go", func(t *testing.T) {
		testExtraneousOrMissingQuoteInQuotedField(t, stage2ParseMasks)
	})
	t.Run("avx2", func(t *testing.T) {
		testExtraneousOrMissingQuoteInQuotedField(t, stage2_parse_test)
	})
}

func testStage2SkipEmptyLines(t *testing.T, f func(input *inputStage2, offset uint64, output *outputStage2)) {

	const file = `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,ccccccccccccccccccccccccccccccc,ddddddddddddddddddddddddddddddd

eeeeeeeeeeeeeeeeeeeeeeeeeeeeee,fffffffffffffffffffffffffffffff,ggggggggggggggggggggggggggggggg,hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh


iiiiiiiiiiiiiiiiiiiiiiiiiiiii,jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj,kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk
lllllllllllllllllllllllllllllll
`

	columns, rows, _ := stage2Parse([]byte(file), '\n', ',', '"', f)
	expectedCols := []uint64{0, 0x1f, 0x20, 0x1f, 0x40, 0x1f, 0x60, 0x1f, 0x0, 0x0, 0x81, 0x1e, 0xa0, 0x1f, 0xc0, 0x1f, 0xe0, 0x1f, 0x0, 0x0, 0x0, 0x0, 0x102, 0x1d, 0x120, 0x1f, 0x140, 0x1f, 0x160, 0x1f}
	expectedRows := []uint64{0, 4,
		// single line skipped
		5, 4,
		// two lines skipped
		11, 3,
		14, 1}

	if !reflect.DeepEqual(columns, expectedCols) {
		t.Errorf("testStage2EmptyLines: got: %v want: %v", columns, expectedCols)
	}

	if !reflect.DeepEqual(rows, expectedRows) {
		t.Errorf("testStage2EmptyLines: got: %v want: %v", rows, expectedRows)
	}
}

func TestStage2SkipEmptyLines(t *testing.T) {
	t.Run("go", func(t *testing.T) {
		testStage2SkipEmptyLines(t, stage2ParseMasks)
	})
	t.Run("avx2", func(t *testing.T) {
		testStage2SkipEmptyLines(t, stage2_parse_test)
	})
}

func TestStage2MissingLastDelimiter(t *testing.T) {

	const file = `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,ccccccccccccccccccccccccccccccc,ddddddddddddddddddddddddddddddd
eeeeeeeeeeeeeeeeeeeeeeeeeeeeeee,fffffffffffffffffffffffffffffff,ggggggggggggggggggggggggggggggg,hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh`

	max := 1000
	if testing.Short() {
		max = 1
	}

	for j := 0; j <= max; j++ {
		for i := 1; i <= len(file); i++ {
			buf := []byte(file[:i])

			masks, _, _ := stage1PreprocessBuffer(buf, ',', 0)
			simdrecords, parsingError := stage2ParseBuffer(buf, masks, '\n', nil)
			if parsingError {
				t.Errorf("TestStage2MissingLastDelimiter: got %v, want %v", parsingError, false)
			}

			r := csv.NewReader(bytes.NewReader(buf))
			r.FieldsPerRecord = -1
			records, _ := r.ReadAll()

			runtime.GC()

			if !reflect.DeepEqual(simdrecords, records) {
				t.Errorf("TestStage2MissingLastDelimiter: got: %v want: %v", simdrecords, records)
			}
		}
	}
}

func TestStage2ParseBuffer(t *testing.T) {

	const vector = `1103341116,2015-12-21T00:00:00,1251,,,CA,200304,,HOND,PA,GY,13147 WELBY WAY,01521,1,4000A1,"NO EVIDENCE,OF REG",50,99999,99999
1103700150,2015-12-21T00:00:00,1435,,,CA,201512,,GMC,VN,WH,525 S MAIN ST,1C51,1,4000A1,NO EVIDENCE OF REG,50,99999,99999
1104803000,2015-12-21T00:00:00,2055,,,CA,201503,,NISS,PA,BK,200 WORLD WAY,2R2,2,8939,WHITE CURB,58,6439997.9,1802686.4
1104820732,2015-12-26T00:00:00,1515,,,CA,,,ACUR,PA,WH,100 WORLD WAY,2F11,2,000,17104h,,6440041.1,1802686.2
1105461453,2015-09-15T00:00:00,115,,,CA,200316,,CHEV,PA,BK,GEORGIA ST/OLYMPIC,1FB70,1,8069A,NO STOPPING/STANDING,93,99999,99999
1106226590,2015-09-15T00:00:00,19,,,CA,201507,,CHEV,VN,GY,SAN PEDRO S/O BOYD,1A35W,1,4000A1,NO EVIDENCE OF REG,50,99999,99999
1106500452,2015-12-17T00:00:00,1710,,,CA,201605,,MAZD,PA,BL,SUNSET/ALVARADO,00217,1,8070,"PARK IN GRID LOCK ZN",163,99999,99999
1106500463,2015-12-17T00:00:00,1710,,,CA,201602,,TOYO,PA,BK,SUNSET/ALVARADO,00217,1,8070,PARK IN GRID LOCK ZN,163,99999,99999
1106506402,2015-12-22T00:00:00,945,,,CA,201605,,CHEV,PA,BR,721 S WESTLAKE,2A75,1,8069AA,NO STOP/STAND AM,93,99999,99999
1106506413,2015-12-22T00:00:00,1100,,,CA,201701,,NISS,PA,SI,1159 HUNTLEY DR,2A75,1,8069AA,NO STOP/STAND AM,93,99999,99999
`

	loops := 25
	if !testing.Short() {
		loops *= 10
	}

	for count := 1; count < loops; count++ {

		buf := []byte(strings.Repeat(vector, count))
		masks, _, _ := stage1PreprocessBuffer(buf, ',', 0)
		simdrecords, parsingError := stage2ParseBuffer(buf, masks, '\n', nil)
		if parsingError {
			t.Errorf("TestStage2ParseBuffer: got %v, want %v", parsingError, false)
		}

		r := csv.NewReader(bytes.NewReader(buf))
		records, err := r.ReadAll()
		if err != nil {
			log.Fatalf("encoding/csv: %v", err)
		}

		runtime.GC()

		if !reflect.DeepEqual(simdrecords, records) {
			t.Errorf("TestParseBlockSecondPass: got %v, want %v", simdrecords, records)
		}
	}
}

func testStage2DynamicAllocation(t *testing.T, init [3]int, expected [3]int) {

	buf, err := ioutil.ReadFile("testdata/parking-citations-100K.csv")
	if err != nil {
		log.Fatalln(err)
	}

	rows := make([]uint64, init[0])
	columns := make([]string, init[1])
	records := make([][]string, 0, init[2])
	var parsingError bool

	masks, _, _ := stage1PreprocessBuffer(buf, ',', 0)
	records, rows, columns, parsingError = stage2ParseBufferEx(buf, masks, '\n', &records, &rows, &columns)

	if cap(rows) != expected[0] {
		t.Errorf("testStage2DynamicAllocation: got %d, want %d", cap(rows), expected[0])
	}
	if cap(columns) != expected[1] {
		t.Errorf("testStage2DynamicAllocation: got %d, want %d", cap(columns), expected[1])
	}
	if parsingError {
		t.Errorf("testStage2DynamicAllocation: got %v, want %v", parsingError, false)
	}

	// we rely on append() for growing the records slice, so use len() instead of cap()
	if len(records) != expected[2] {
		t.Errorf("testStage2DynamicAllocation: got %d, want %d", len(records), expected[2])
	}
}

// Check that the buffers are increased dynamically
func TestStage2DynamicAllocation(t *testing.T) {
	t.Run("grow-rows", func(t *testing.T) {
		testStage2DynamicAllocation(t, [3]int{128, 10000 * 20 * 2, 10000}, [3]int{262144, 3200000, 100000})
	})
	t.Run("grow-columns", func(t *testing.T) {
		testStage2DynamicAllocation(t, [3]int{10000 * 4, 1024, 10000}, [3]int{320000, 2097152, 100000})
	})
	t.Run("grow-records", func(t *testing.T) {
		testStage2DynamicAllocation(t, [3]int{10000 * 4, 10000 * 20 * 2, 100}, [3]int{320000, 3200000, 100000})
	})
}

func BenchmarkStage2Parsing(b *testing.B) {
	b.Run("parking-citations-100K", func(b *testing.B) {
		benchmarkStage2Parsing(b, "testdata/parking-citations-100K.csv", 100000, 20)
	})
	b.Run("worldcitiespop-100K", func(b *testing.B) {
		benchmarkStage2Parsing(b, "testdata/worldcitiespop-100K.csv", 100000, 20)
	})
	b.Run("nyc-taxi-data-100K", func(b *testing.B) {
		benchmarkStage2Parsing(b, "testdata/nyc-taxi-data-100K.csv", 100000, 25)
	})
}

func benchmarkStage2Parsing(b *testing.B, filename string, lines, fields int) {

	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalln(err)
	}

	rows := make([]uint64, (lines*17>>4)*2)
	columns := make([]string, len(rows)*fields)
	simdrecords := make([][]string, 0, len(rows))

	masks, _, _ := stage1PreprocessBuffer(buf, ',', 0)

	b.SetBytes(int64(len(buf)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		stage2ParseBufferEx(buf, masks, '\n', &simdrecords, &rows, &columns)
	}
}

func BenchmarkStagesCombined(b *testing.B) {
	b.Run("parking-citations-100K", func(b *testing.B) {
		benchmarkStagesCombined(b, "testdata/parking-citations-100K.csv", 100000, 20)
	})
	b.Run("worldcitiespop-100K", func(b *testing.B) {
		benchmarkStagesCombined(b, "testdata/worldcitiespop-100K.csv", 100000, 20)
	})
	b.Run("nyc-taxi-data-100K", func(b *testing.B) {
		benchmarkStagesCombined(b, "testdata/nyc-taxi-data-100K.csv", 100000, 25)
	})
}

func benchmarkStagesCombined(b *testing.B, filename string, lines, fields int) {

	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalln(err)
	}

	masks := allocMasks(buf)
	postProc := make([]uint64, 0, len(buf)>>6)

	b.SetBytes(int64(len(buf)))
	b.ResetTimer()

	rows := make([]uint64, (lines*17>>4)*2)
	columns := make([]string, len(rows)*fields)
	simdrecords := make([][]string, 0, lines)

	for i := 0; i < b.N; i++ {
		input, output := stage1Input{}, stage1Output{}
		postProc = postProc[:0]
		stage1_preprocess_buffer(buf, uint64(','), &input, &output, &postProc, 0, masks, 0)
		stage2ParseBufferEx(buf, masks, '\n', &simdrecords, &rows, &columns)
	}
}
