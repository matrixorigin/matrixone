// Copyright 2026 Matrix Origin
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

package bytejsonvalidate

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"unicode/utf8"
)

func TestContainerNumericEncoding(t *testing.T) {
	nested := testScalarArray([]byte{typeInt64, typeUint64, typeFloat64}, [][]byte{make([]byte, 8), make([]byte, 8), make([]byte, 8)})
	data := testScalarArray([]byte{typeLiteral, typeInt64, typeArray, typeString}, [][]byte{{1}, make([]byte, 8), nested, {1, 'x'}})
	for i := range data {
		for _, value := range []byte{0, 1, 2, typeInt64, typeUint64, typeFloat64, 0xfd, 0xff} {
			d := append([]byte(nil), data...)
			d[i] = value
			want := Container(typeArray, d, benchmarkScalar)
			if got := ContainerWithNumericEncoding(typeArray, d, benchmarkScalar); got != want {
				t.Fatalf("byte%d=%x got%v want%v", i, value, got, want)
			}
		}
	}
	for _, floating := range []float64{0, math.Copysign(0, -1), math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64, math.MaxFloat64, -math.MaxFloat64, math.Inf(1), math.Inf(-1), math.NaN()} {
		payload := make([]byte, 8)
		binary.LittleEndian.PutUint64(payload, math.Float64bits(floating))
		d := testScalarArray([]byte{typeFloat64}, [][]byte{payload})
		want := !math.IsInf(floating, 0) && !math.IsNaN(floating)
		if got := ContainerWithNumericEncoding(typeArray, d, benchmarkScalar); got != want {
			t.Fatalf("float%v got%v want%v", floating, got, want)
		}
	}
	// The original generic API still calls and honors the integer predicate.
	integer := testScalarArray([]byte{typeInt64}, [][]byte{make([]byte, 8)})
	visits := 0
	if Container(typeArray, integer, func(byte, []byte) bool { visits++; return false }) || visits != 1 {
		t.Fatal("generic integer callback changed")
	}
	if !ContainerWithNumericEncoding(typeArray, integer, func(byte, []byte) bool { t.Fatal("integer callback"); return false }) {
		t.Fatal("valid integer rejected")
	}
	// A nonnumeric callback can corrupt a later descriptor. No prefetch or copy.
	d := testScalarArray([]byte{typeString, typeInt64}, [][]byte{{1, 'x'}, make([]byte, 8)})
	visits = 0
	if ContainerWithNumericEncoding(typeArray, d, func(tp byte, p []byte) bool { visits++; d[13] = 0xfd; return benchmarkScalar(tp, p) }) || visits != 1 {
		t.Fatal("later descriptor corruption bypassed")
	}
	// The specialized path retains the serialized-byte and nesting bounds.
	if ContainerWithNumericEncoding(typeArray, testWrapArray(integer, 512, true), benchmarkScalar) {
		t.Fatal("alias expansion accepted")
	}
	deep := integer
	for i := 0; i < maxContainerDepth; i++ {
		deep = testWrapArray(deep, 1, false)
	}
	if ContainerWithNumericEncoding(typeArray, deep, benchmarkScalar) {
		t.Fatal("depth bound bypassed")
	}
	t.Run("numeric prefix stops at callback", func(t *testing.T) {
		for _, repair := range []bool{false, true} {
			d := testScalarArray([]byte{typeInt64, typeUint64, typeString, typeInt64}, [][]byte{make([]byte, 8), make([]byte, 8), {1, 'x'}, make([]byte, 8)})
			if repair {
				d[headerSize+3*valEntrySize] = 0xfd
			}
			visits := 0
			got := ContainerWithNumericEncoding(typeArray, d, func(tp byte, scalar []byte) bool {
				visits++
				if tp != typeString {
					t.Fatalf("unexpected callback type %x", tp)
				}
				if repair {
					d[headerSize+3*valEntrySize] = typeInt64
				} else {
					d[headerSize+3*valEntrySize] = 0xfd
				}
				return benchmarkScalar(tp, scalar)
			})
			if got != repair || visits != 1 {
				t.Fatalf("repair=%v valid=%v callbacks=%d", repair, got, visits)
			}
		}
	})
	t.Run("numeric prefix charges before callback", func(t *testing.T) {
		const count = 9
		payload := headerSize + count*valEntrySize
		d := make([]byte, payload+10)
		binary.LittleEndian.PutUint32(d, count)
		binary.LittleEndian.PutUint32(d[docSizeOff:], uint32(len(d)))
		for i := 0; i < count-1; i++ {
			d[headerSize+i*valEntrySize] = typeInt64
			binary.LittleEndian.PutUint32(d[headerSize+i*valEntrySize+valTypeSize:], uint32(payload))
		}
		d[headerSize+(count-1)*valEntrySize] = typeString
		binary.LittleEndian.PutUint32(d[headerSize+(count-1)*valEntrySize+valTypeSize:], uint32(payload+8))
		d[payload+8], d[payload+9] = 1, 'x'
		if ContainerWithNumericEncoding(typeArray, d, func(byte, []byte) bool {
			t.Fatal("byte budget must reject before the nonnumeric callback")
			return true
		}) {
			t.Fatal("numeric alias expansion accepted")
		}
	})

}

func TestChildValueVariableLengthFraming(t *testing.T) {
	for _, size := range []int{0, 1, 127, 128, 255, 16383, 16384} {
		prefix := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(prefix, uint64(size))
		encoded := append(prefix[:n:n], make([]byte, size)...)
		for _, tp := range []byte{typeString, typeDecimal, typeDate, typeTime, typeDatetime, typeBlob, typeOpaque, typeBit} {
			withTail := append(append([]byte(nil), encoded...), 0xff)
			value, valid := childValue(tp, withTail)
			if !valid || !bytes.Equal(value, encoded) {
				t.Fatalf("type=%x size=%d valid=%v length=%d", tp, size, valid, len(value))
			}
			if _, valid := childValue(tp, encoded[:len(encoded)-1]); valid {
				t.Fatalf("truncated type=%x size=%d accepted", tp, size)
			}
			// Preserve the encoded number with an extra zero group. The
			// descendant boundary must reject it before invoking a callback.
			overlong := append([]byte(nil), encoded[:n]...)
			overlong[n-1] |= 0x80
			overlong = append(overlong, 0)
			overlong = append(overlong, encoded[n:]...)
			if _, valid := childValue(tp, overlong); valid {
				t.Fatalf("nonshortest type=%x size=%d accepted", tp, size)
			}
		}
	}
}

func TestUvarintPayloadFraming(t *testing.T) {
	for _, tc := range []struct {
		name    string
		data    []byte
		payload []byte
		valid   bool
	}{
		{"empty payload", []byte{0}, []byte{}, true},
		{"one byte", []byte{1, 'x'}, []byte{'x'}, true},
		{"opaque bytes", []byte{1, 0xff}, []byte{0xff}, true},
		{"127", append([]byte{127}, bytes.Repeat([]byte{'x'}, 127)...), bytes.Repeat([]byte{'x'}, 127), true},
		{"128", append([]byte{0x80, 1}, bytes.Repeat([]byte{'x'}, 128)...), bytes.Repeat([]byte{'x'}, 128), true},
		{"16383", append([]byte{0xff, 0x7f}, bytes.Repeat([]byte{'x'}, 16383)...), bytes.Repeat([]byte{'x'}, 16383), true},
		{"16384", append([]byte{0x80, 0x80, 1}, bytes.Repeat([]byte{'x'}, 16384)...), bytes.Repeat([]byte{'x'}, 16384), true},
		{"missing", nil, nil, false},
		{"extra payload", []byte{0, 'x'}, nil, false},
		{"short payload", []byte{2, 'x'}, nil, false},
		{"unterminated", []byte{0x80}, nil, false},
		{"overlong empty", []byte{0x80, 0}, nil, false},
		{"overlong one", []byte{0x81, 0, 'x'}, nil, false},
		{"overflow", append(bytes.Repeat([]byte{0xff}, 9), 2), nil, false},
		{"huge missing payload", append(bytes.Repeat([]byte{0xff}, 9), 1), nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, ok := UvarintPayload(tc.data)
			if ok != tc.valid || !bytes.Equal(payload, tc.payload) || !ok && payload != nil {
				t.Fatalf("got payload=%x valid=%v; want payload=%x valid=%v", payload, ok, tc.payload, tc.valid)
			}
			if len(payload) > 0 && &payload[0] != &tc.data[len(tc.data)-len(payload)] {
				t.Fatal("payload must remain a view of the original input")
			}
		})
	}
	for width := 2; width <= binary.MaxVarintLen64; width++ {
		overlong := append(bytes.Repeat([]byte{0x80}, width-1), 0)
		if _, ok := UvarintPayload(overlong); ok {
			t.Fatalf("accepted overlong zero with prefix width%d", width)
		}
	}
}

func TestDecimalJSONPayloadFraming(t *testing.T) {
	for _, tc := range []struct {
		data  []byte
		valid bool
	}{
		{[]byte{1, '1'}, true},
		{[]byte{4, '-', '1', '.', '5'}, true},
		{append([]byte{0x80, 1}, bytes.Repeat([]byte{'1'}, 128)...), true},
		{[]byte{0}, false},
		{[]byte{1, 'x'}, false},
		{[]byte{2, '1', ' '}, false},
		{[]byte{0x81, 0, '1'}, false},
		{[]byte{2, '1'}, false},
	} {
		if got := DecimalJSONPayload(tc.data); got != tc.valid {
			t.Fatalf("data=%x valid=%v; want%v", tc.data, got, tc.valid)
		}
	}
}

func testLiteralArray() []byte {
	data := make([]byte, 18)
	binary.LittleEndian.PutUint32(data, 2)
	binary.LittleEndian.PutUint32(data[4:], uint32(len(data)))
	data[8], data[13] = typeLiteral, typeLiteral
	return data
}

// Each payload is separately encoded; offsets are relative to this container.
func testScalarArray(types []byte, payloads [][]byte) []byte {
	size := headerSize + len(types)*valEntrySize
	for i, tp := range types {
		if tp != typeLiteral {
			size += len(payloads[i])
		}
	}
	data := make([]byte, size)
	binary.LittleEndian.PutUint32(data, uint32(len(types)))
	binary.LittleEndian.PutUint32(data[docSizeOff:], uint32(size))
	offset := headerSize + len(types)*valEntrySize
	for i, tp := range types {
		entry := headerSize + i*valEntrySize
		data[entry] = tp
		if tp == typeLiteral {
			data[entry+1] = payloads[i][0]
		} else {
			binary.LittleEndian.PutUint32(data[entry+1:], uint32(offset))
			copy(data[offset:], payloads[i])
			offset += len(payloads[i])
		}
	}
	return data
}

func TestContainerScalarCallbacks(t *testing.T) {
	for _, stored := range []bool{false, true} {
		t.Run(fmt.Sprintf("stored=%v", stored), func(t *testing.T) {
			child := testScalarArray([]byte{typeUint64, typeFloat64}, [][]byte{{2, 0, 0, 0, 0, 0, 0, 0}, {3, 0, 0, 0, 0, 0, 0, 0}})
			data := testScalarArray([]byte{typeLiteral, typeInt64, typeArray, typeString}, [][]byte{{4}, {1, 0, 0, 0, 0, 0, 0, 0}, child, {1, 'x'}})
			wantTypes := []byte{typeLiteral, typeInt64, typeUint64, typeFloat64, typeString}
			wantData := [][]byte{{4}, {1, 0, 0, 0, 0, 0, 0, 0}, {2, 0, 0, 0, 0, 0, 0, 0}, {3, 0, 0, 0, 0, 0, 0, 0}, {1, 'x'}}
			// Rejection at each callback must stop before the next callback.
			for reject := -1; reject < len(wantTypes); reject++ {
				visits := 0
				predicate := func(tp byte, scalar []byte) bool {
					if visits >= len(wantTypes) || tp != wantTypes[visits] || !bytes.Equal(scalar, wantData[visits]) {
						t.Fatalf("callback %d: type=%x data=%x", visits, tp, scalar)
					}
					visits++
					return visits-1 != reject
				}
				valid := false
				if stored {
					valid, _ = StoredContainer(typeArray, data, predicate, 13)
				} else {
					valid = Container(typeArray, data, predicate)
				}
				wantVisits := len(wantTypes)
				if reject >= 0 {
					wantVisits = reject + 1
				}
				if valid != (reject == -1) || visits != wantVisits {
					t.Fatalf("reject=%d valid=%v visits=%d want=%d", reject, valid, visits, wantVisits)
				}
			}
		})
	}
}

func TestContainerMutableValueTable(t *testing.T) {
	for _, stored := range []bool{false, true} {
		for _, change := range []string{"next-literal", "next-type", "header-count"} {
			t.Run(fmt.Sprintf("stored=%v/%s", stored, change), func(t *testing.T) {
				data := testLiteralArray()
				visits := 0
				predicate := func(tp byte, scalar []byte) bool {
					if visits >= 2 {
						t.Fatal("extra callback")
					}
					want := byte(0)
					if visits == 1 && change == "next-literal" {
						want = 2
					}
					wantCap := cap(data) - headerSize - visits*valEntrySize - valTypeSize
					if tp != typeLiteral || len(scalar) != 1 || cap(scalar) != wantCap || scalar[0] != want {
						t.Fatalf("callback %d: type=%x len=%d cap=%d data=%x", visits, tp, len(scalar), cap(scalar), scalar)
					}
					if visits == 0 {
						// The callback can reslice its borrowed bytes into the next
						// entry. Later entries must not be copied or prefetched.
						switch change {
						case "next-literal":
							scalar[:cap(scalar)][valEntrySize] = 2
						case "next-type":
							scalar[:cap(scalar)][valEntrySize-valTypeSize] = 0xfd
						case "header-count":
							// The original walk captures the count before callbacks.
							binary.LittleEndian.PutUint32(data, 0)
						}
					}
					visits++
					return true
				}
				valid, depthExceeded := false, false
				if stored {
					valid, depthExceeded = StoredContainer(typeArray, data, predicate, 3)
				} else {
					valid = Container(typeArray, data, predicate)
				}
				wantValid, wantVisits := change != "next-type", 2
				if !wantValid {
					wantVisits = 1
				}
				if valid != wantValid || depthExceeded || visits != wantVisits {
					t.Fatalf("valid=%v depthExceeded=%v visits=%d want=%v/%d", valid, depthExceeded, visits, wantValid, wantVisits)
				}
			})
		}
		for _, tp := range []byte{typeInt64, typeUint64, typeFloat64} {
			for _, rejectLast := range []bool{false, true} {
				t.Run(fmt.Sprintf("stored=%v/numeric-borrow/type=%x/reject=%v", stored, tp, rejectLast), func(t *testing.T) {
					data := testScalarArray([]byte{tp, tp}, [][]byte{{1, 0, 0, 0, 0, 0, 0, 0}, {2, 0, 0, 0, 0, 0, 0, 0}})
					visits := 0
					predicate := func(got byte, scalar []byte) bool {
						if visits >= 2 {
							t.Fatal("extra numeric callback")
						}
						want := uint64(1)
						if visits == 1 {
							want = 3
						}
						wantCap := cap(data) - (headerSize + 2*valEntrySize + visits*numberSize)
						if got != tp || len(scalar) != numberSize || cap(scalar) != wantCap || binary.LittleEndian.Uint64(scalar) != want {
							t.Fatalf("numeric callback %d: type=%x len=%d cap=%d data=%x", visits, got, len(scalar), cap(scalar), scalar)
						}
						if visits == 0 {
							// Exact length is not a capacity fence. Mutating the
							// next payload must be visible to its later callback.
							scalar[:cap(scalar)][numberSize] = 3
						}
						visits++
						return !rejectLast || visits != 2
					}
					valid, depthExceeded := false, false
					if stored {
						valid, depthExceeded = StoredContainer(typeArray, data, predicate, 5)
					} else {
						valid = Container(typeArray, data, predicate)
					}
					if valid == rejectLast || depthExceeded || visits != 2 {
						t.Fatalf("numeric borrowed callback: valid=%v depth=%v visits=%d", valid, depthExceeded, visits)
					}
				})
			}
		}
	}
}

func TestContainerNumericMutableDescriptors(t *testing.T) {
	for _, stored := range []bool{false, true} {
		for _, change := range []string{"type", "unknown", "truncated", "overlap", "nonfinite", "header"} {
			t.Run(fmt.Sprintf("stored=%v/%s", stored, change), func(t *testing.T) {
				data := testScalarArray([]byte{typeInt64, typeInt64}, [][]byte{make([]byte, numberSize), make([]byte, numberSize)})
				second := headerSize + valEntrySize
				payload := headerSize + 2*valEntrySize
				visits := 0
				predicate := func(tp byte, scalar []byte) bool {
					visits++
					if visits == 1 {
						switch change {
						case "type":
							data[second] = typeUint64
						case "unknown":
							data[second] = 0xfd
						case "truncated":
							binary.LittleEndian.PutUint32(data[second+1:], uint32(len(data)-numberSize+1))
						case "overlap":
							binary.LittleEndian.PutUint32(data[second+1:], uint32(payload))
						case "nonfinite":
							data[second] = typeFloat64
							binary.LittleEndian.PutUint64(data[payload+numberSize:], math.Float64bits(math.Inf(1)))
						case "header":
							binary.LittleEndian.PutUint32(data, 0)
							binary.LittleEndian.PutUint32(data[docSizeOff:], 0)
						}
					} else if visits != 2 {
						t.Fatal("extra callback")
					} else if change == "type" && tp != typeUint64 || change == "nonfinite" && tp != typeFloat64 {
						t.Fatalf("stale descriptor: type=%x", tp)
					}
					return benchmarkScalar(tp, scalar)
				}
				valid, depthExceeded := false, false
				if stored {
					valid, depthExceeded = StoredContainer(typeArray, data, predicate, 5)
				} else {
					valid = Container(typeArray, data, predicate)
				}
				wantValid := change == "type" || change == "header" || change == "overlap" && !stored
				wantVisits := 2
				if change == "unknown" || change == "truncated" || change == "overlap" && stored {
					wantVisits = 1
				}
				if valid != wantValid || depthExceeded || visits != wantVisits {
					t.Fatalf("valid=%v depth=%v visits=%d want=%v/%d", valid, depthExceeded, visits, wantValid, wantVisits)
				}
			})
		}
	}
}

func TestContainerNumericReentrantMutation(t *testing.T) {
	for _, stored := range []bool{false, true} {
		for _, reject := range []bool{false, true} {
			t.Run(fmt.Sprintf("stored=%v/reject=%v", stored, reject), func(t *testing.T) {
				data := testScalarArray([]byte{typeInt64, typeInt64}, [][]byte{make([]byte, numberSize), make([]byte, numberSize)})
				validate := func(predicate func(byte, []byte) bool) bool {
					if stored {
						valid, depth := StoredContainer(typeArray, data, predicate, 5)
						if depth {
							t.Fatal("unexpected depth result")
						}
						return valid
					}
					return Container(typeArray, data, predicate)
				}
				outer, inner := 0, 0
				valid := validate(func(tp byte, scalar []byte) bool {
					outer++
					if outer == 1 {
						nested := validate(func(tp byte, scalar []byte) bool {
							inner++
							if inner == 1 {
								data[headerSize+valEntrySize] = typeUint64
								if reject {
									data[headerSize+valEntrySize] = 0xfd
								}
							} else if tp != typeUint64 {
								t.Fatalf("stale inner descriptor: %x", tp)
							}
							return benchmarkScalar(tp, scalar)
						})
						if nested == reject {
							t.Fatalf("nested valid=%v reject=%v", nested, reject)
						}
					} else if tp != typeUint64 {
						t.Fatalf("stale outer descriptor: %x", tp)
					}
					return benchmarkScalar(tp, scalar)
				})
				wantVisits := 2
				if reject {
					wantVisits = 1
				}
				if valid == reject || outer != wantVisits || inner != wantVisits {
					t.Fatalf("valid=%v outer=%d inner=%d want=%d", valid, outer, inner, wantVisits)
				}
			})
		}
	}
}

func TestContainerScalarBounds(t *testing.T) {
	for _, tp := range []byte{typeInt64, typeUint64, typeFloat64, typeString, typeDecimal, typeDate, typeTime, typeDatetime, typeBlob, typeOpaque, typeBit, typeArray, typeObject, 0xfd} {
		t.Run(fmt.Sprintf("type=%x", tp), func(t *testing.T) {
			payload := []byte{1, 'x'}
			validType := tp != 0xfd
			if tp == typeInt64 || tp == typeUint64 || tp == typeFloat64 {
				payload = make([]byte, numberSize)
			} else if tp == typeArray || tp == typeObject {
				payload = make([]byte, headerSize)
				binary.LittleEndian.PutUint32(payload[docSizeOff:], headerSize)
			}
			for _, position := range []int{0, 1, 2} {
				for _, bad := range []string{"valid", "table", "end", "wide", "truncated"} {
					t.Run(fmt.Sprintf("position=%d/%s", position, bad), func(t *testing.T) {
						// Replace the chosen scalar while retaining valid neighbors.
						types := []byte{typeInt64, typeInt64, typeInt64}
						payloads := [][]byte{make([]byte, 8), make([]byte, 8), make([]byte, 8)}
						types[position], payloads[position] = tp, payload
						data := testScalarArray(types, payloads)
						entry := headerSize + position*valEntrySize
						switch bad {
						case "table":
							binary.LittleEndian.PutUint32(data[entry+1:], headerSize+3*valEntrySize-1)
						case "end":
							binary.LittleEndian.PutUint32(data[entry+1:], uint32(len(data)))
						case "wide":
							binary.LittleEndian.PutUint32(data[entry+1:], math.MaxUint32)
						case "truncated":
							// Leave less than the required scalar/container framing.
							offset := len(data) - len(payload) + 1
							copy(data[offset:], payload[:len(payload)-1])
							binary.LittleEndian.PutUint32(data[entry+1:], uint32(offset))
						}
						for _, stored := range []bool{false, true} {
							visits := 0
							accept := func(byte, []byte) bool { visits++; return true }
							valid := false
							if stored {
								valid, _ = StoredContainer(typeArray, data, accept, 10)
							} else {
								valid = Container(typeArray, data, accept)
							}
							wantValid := bad == "valid" && validType
							wantVisits := position
							if wantValid {
								wantVisits = 3
								if tp == typeArray || tp == typeObject {
									wantVisits--
								}
							}
							if valid != wantValid || visits != wantVisits {
								t.Fatalf("stored=%v valid=%v visits=%d want=%v/%d", stored, valid, visits, wantValid, wantVisits)
							}
						}
					})
				}
			}
		})
	}
}

func TestContainerNumericPredicatesAndBudgets(t *testing.T) {
	for _, tp := range []byte{typeInt64, typeUint64, typeFloat64} {
		data := testScalarArray([]byte{tp, tp}, [][]byte{make([]byte, 8), make([]byte, 8)})
		for _, stored := range []bool{false, true} {
			for _, short := range []bool{false, true} {
				remaining, work := uint64(len(data)), uint64(5)
				if short {
					remaining--
				}
				var storedWork *uint64
				if stored {
					storedWork = &work
				}
				visits := 0
				valid := container(typeArray, data, func(byte, []byte) bool { visits++; return true }, 1, &remaining, storedWork, nil, false, false)
				wantVisits := 2
				wantRemaining := uint64(0)
				if short {
					wantVisits = 1
					wantRemaining = 7
				}
				if valid == short || visits != wantVisits || remaining != wantRemaining || stored && work != 0 {
					t.Fatalf("type=%x stored=%v short=%v valid=%v visits=%d remaining=%d work=%d", tp, stored, short, valid, visits, remaining, work)
				}
			}
		}
		visits := 0
		valid, _ := StoredContainer(typeArray, data, func(byte, []byte) bool { visits++; return true }, 4)
		if valid || visits != 1 {
			t.Fatalf("type=%x node budget valid=%v visits=%d", tp, valid, visits)
		}
	}
	for _, value := range []float64{1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		payload := make([]byte, 8)
		binary.LittleEndian.PutUint64(payload, math.Float64bits(value))
		data := testScalarArray([]byte{typeFloat64}, [][]byte{payload})
		finite := !math.IsNaN(value) && !math.IsInf(value, 0)
		predicate := func(tp byte, data []byte) bool {
			got := math.Float64frombits(binary.LittleEndian.Uint64(data))
			return tp == typeFloat64 && len(data) == 8 && !math.IsNaN(got) && !math.IsInf(got, 0)
		}
		valid, _ := StoredContainer(typeArray, data, predicate, 3)
		if Container(typeArray, data, predicate) != finite || valid != finite {
			t.Fatalf("finite predicate bypassed: %v", value)
		}
	}
}

func TestObjectKeyBudgetRejectsAliasAmplification(t *testing.T) {
	const count, keyLength = 64, 1024
	payloadStart := headerSize + count*(keyEntrySize+valEntrySize)
	data := make([]byte, payloadStart+keyLength)
	binary.LittleEndian.PutUint32(data, count)
	binary.LittleEndian.PutUint32(data[docSizeOff:], uint32(len(data)))
	for i := 0; i < count; i++ {
		entry := headerSize + i*keyEntrySize
		binary.LittleEndian.PutUint32(data[entry:], uint32(payloadStart))
		binary.LittleEndian.PutUint16(data[entry+keyOriginOff:], keyLength)
		value := headerSize + count*keyEntrySize + i*valEntrySize
		data[value], data[value+valTypeSize] = typeLiteral, 1
	}
	for i := payloadStart; i < len(data); i++ {
		data[i] = 'a'
	}
	for _, stored := range []bool{false, true} {
		visits := 0
		predicate := func(byte, []byte) bool { visits++; return true }
		valid := false
		if stored {
			valid, _ = StoredContainer(typeObject, data, predicate, uint64(len(data))*4)
		} else {
			valid = Container(typeObject, data, predicate)
		}
		if valid || visits != 0 {
			t.Fatalf("stored=%v valid=%v scalar callbacks=%d", stored, valid, visits)
		}
	}
}

func testObjectWithTwoKeys(first, gap, second []byte) []byte {
	const count = 2
	payloadStart := headerSize + count*(keyEntrySize+valEntrySize)
	data := make([]byte, payloadStart+len(first)+len(gap)+len(second))
	binary.LittleEndian.PutUint32(data, count)
	binary.LittleEndian.PutUint32(data[docSizeOff:], uint32(len(data)))
	offset := payloadStart
	for i, key := range [][]byte{first, second} {
		if i == 1 {
			copy(data[offset:], gap)
			offset += len(gap)
		}
		entry := headerSize + i*keyEntrySize
		binary.LittleEndian.PutUint32(data[entry:], uint32(offset))
		binary.LittleEndian.PutUint16(data[entry+keyOriginOff:], uint16(len(key)))
		copy(data[offset:], key)
		offset += len(key)
		value := headerSize + count*keyEntrySize + i*valEntrySize
		data[value], data[value+valTypeSize] = typeLiteral, 1
	}
	return data
}

func TestCanonicalObjectUTF8KeyBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name       string
		first, gap []byte
		second     []byte
		valid      bool
	}{
		{"unicode", []byte("é"), nil, []byte("猫"), true},
		{"empty first", nil, nil, []byte("a"), true},
		{"invalid padding ignored", []byte("a"), []byte{0xff}, []byte("b"), true},
		{"split two-byte rune", []byte{'a', 0xc3}, nil, []byte{0xa9}, false},
		{"split three-byte rune", []byte{'a', 0xe7, 0x8c}, nil, []byte{0xab}, false},
		{"overlong rune", []byte("a"), nil, []byte{'b', 0xc0, 0xaf}, false},
		{"unsorted", []byte("b"), nil, []byte("a"), false},
		{"duplicate", []byte("a"), nil, []byte("a"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := testObjectWithTwoKeys(tc.first, tc.gap, tc.second)
			for _, validate := range []func(byte, []byte, func(byte, []byte) bool, uint64) (bool, bool){
				StoredContainerWithNumericEncoding, StoredContainerWithCanonicalStrings,
			} {
				visits := 0
				predicate := func(tp byte, value []byte) bool { visits++; return benchmarkScalar(tp, value) }
				valid, depth := validate(typeObject, data, predicate, uint64(len(data))*4)
				if valid != tc.valid || depth || !valid && visits != 0 {
					t.Fatalf("got valid=%v depth=%v callbacks=%d want valid=%v", valid, depth, visits, tc.valid)
				}
			}
		})
	}
	// Independently validate each key and its order, including inputs whose
	// concatenation happens to form valid UTF-8 across an invalid boundary.
	keys := [][]byte{nil, {'a'}, {'b'}, {0}, {0x7f}, {0x80}, {0xa9}, {0xc2}, {0xc3, 0xa9},
		{'a', 0xc3}, {'a', 0xe7, 0x8c}, {0xab}, {0xe7, 0x8c, 0xab}, {'b', 0xff}}
	for _, first := range keys {
		for _, second := range keys {
			for _, gap := range [][]byte{nil, {0xff}} {
				data := testObjectWithTwoKeys(first, gap, second)
				want := utf8.Valid(first) && utf8.Valid(second) && bytes.Compare(first, second) < 0
				got, depth := StoredContainerWithCanonicalStrings(typeObject, data, benchmarkScalar, uint64(len(data))*4)
				if got != want || depth {
					t.Fatalf("first=%x gap=%x second=%x got=%v/%v want=%v", first, gap, second, got, depth, want)
				}
			}
		}
	}
}

func FuzzCanonicalUTF8Runs(f *testing.F) {
	seed := testScalarArray([]byte{typeString, typeString}, [][]byte{{1, 'a'}, {1, 'b'}})
	f.Add([]byte("a"), []byte{0xff}, []byte("b"), seed)
	f.Add([]byte{'a', 0xc3}, []byte{}, []byte{0xa9}, seed)
	f.Add([]byte("é"), []byte{}, []byte("猫"), []byte{})
	f.Fuzz(func(t *testing.T, first, gap, second, arbitrary []byte) {
		if len(first) > 512 || len(gap) > 512 || len(second) > 512 || len(arbitrary) > 4096 {
			return
		}
		predicate := func(tp byte, data []byte) bool {
			if tp == typeString {
				payload, ok := UvarintPayload(data)
				return ok && utf8.Valid(payload)
			}
			return benchmarkScalar(tp, data)
		}
		object := testObjectWithTwoKeys(first, gap, second)
		wantObject := utf8.Valid(first) && utf8.Valid(second) && bytes.Compare(first, second) < 0
		gotObject, depth := StoredContainerWithCanonicalStrings(typeObject, object, predicate, uint64(len(object))*4)
		if gotObject != wantObject || depth {
			t.Fatalf("key oracle got=%v/%v want=%v", gotObject, depth, wantObject)
		}
		encode := func(value []byte) []byte { return append(binary.AppendUvarint(nil, uint64(len(value))), value...) }
		array := testScalarArray([]byte{typeString, typeString}, [][]byte{encode(first), encode(second)})
		secondEntry := headerSize + valEntrySize
		secondOffset := int(binary.LittleEndian.Uint32(array[secondEntry+valTypeSize:]))
		withGap := make([]byte, len(array)+len(gap))
		copy(withGap, array[:secondOffset])
		copy(withGap[secondOffset:], gap)
		copy(withGap[secondOffset+len(gap):], array[secondOffset:])
		binary.LittleEndian.PutUint32(withGap[docSizeOff:], uint32(len(withGap)))
		binary.LittleEndian.PutUint32(withGap[secondEntry+valTypeSize:], uint32(secondOffset+len(gap)))
		wantArray := utf8.Valid(first) && utf8.Valid(second)
		gotArray, depth := StoredContainerWithCanonicalStrings(typeArray, withGap, predicate, uint64(len(withGap))*4)
		if gotArray != wantArray || depth {
			t.Fatalf("string oracle got=%v/%v want=%v", gotArray, depth, wantArray)
		}
		want, wantDepth := StoredContainerWithNumericEncoding(typeArray, arbitrary, predicate, uint64(len(arbitrary))*4)
		got, gotDepth := StoredContainerWithCanonicalStrings(typeArray, arbitrary, predicate, uint64(len(arbitrary))*4)
		if got != want || gotDepth != wantDepth {
			t.Fatalf("arbitrary framing got=%v/%v want=%v/%v", got, gotDepth, want, wantDepth)
		}
	})
}

func TestContainerOrdinaryAndCanonicalGuards(t *testing.T) {
	accept := func(byte, []byte) bool { return true }
	// Two sorted keys followed by two distinct eight-byte values.
	object := make([]byte, 48)
	binary.LittleEndian.PutUint32(object, 2)
	binary.LittleEndian.PutUint32(object[4:], 48)
	binary.LittleEndian.PutUint32(object[8:], 30)
	binary.LittleEndian.PutUint16(object[12:], 1)
	binary.LittleEndian.PutUint32(object[14:], 31)
	binary.LittleEndian.PutUint16(object[18:], 1)
	object[20], object[25] = typeInt64, typeUint64
	binary.LittleEndian.PutUint32(object[21:], 32)
	binary.LittleEndian.PutUint32(object[26:], 40)
	object[30], object[31] = 'a', 'b'
	for _, change := range []string{"valid", "duplicate", "unsorted", "invalid-utf8", "overlapping-keys", "overlapping-values"} {
		t.Run(change, func(t *testing.T) {
			data := bytes.Clone(object)
			switch change {
			case "duplicate":
				data[31] = 'a'
			case "unsorted":
				data[30], data[31] = 'b', 'a'
			case "invalid-utf8":
				data[31] = 0xff
			case "overlapping-keys":
				binary.LittleEndian.PutUint32(data[14:], 30)
			case "overlapping-values":
				binary.LittleEndian.PutUint32(data[26:], 32)
			}
			valid, depth := StoredContainer(typeObject, data, accept, 5)
			if !Container(typeObject, data, accept) || valid != (change == "valid") || depth {
				t.Fatalf("ordinary/canonical distinction lost: stored=%v depth=%v", valid, depth)
			}
		})
	}
	for _, payload := range [][]byte{{0}, {1, 'x'}, {0x80, 0}, {0x81, 0, 'x'}, {0x80}, {2, 'x'}} {
		data := testScalarArray([]byte{typeString}, [][]byte{payload})
		want := bytes.Equal(payload, []byte{0}) || bytes.Equal(payload, []byte{1, 'x'})
		visits := 0
		predicate := func(byte, []byte) bool { visits++; return true }
		valid := Container(typeArray, data, predicate)
		wantVisits := 0
		if want {
			wantVisits = 1
		}
		if valid != want || visits != wantVisits {
			t.Fatalf("uvarint payload=%x valid=%v visits=%d", payload, valid, visits)
		}
	}
	for _, data := range [][]byte{nil, make([]byte, 7), {0xff, 0xff, 0xff, 0xff, 8, 0, 0, 0}, {0, 0, 0, 0, 9, 0, 0, 0}} {
		if Container(typeArray, data, func(byte, []byte) bool { t.Fatal("malformed header callback"); return true }) {
			t.Fatalf("malformed header accepted: %x", data)
		}
	}
}

// The callback deliberately retains common scalar checks in both benchmark arms.
func benchmarkScalar(tp byte, data []byte) bool {
	switch tp {
	case typeInt64, typeUint64:
		return len(data) == numberSize
	case typeFloat64:
		value := math.Float64frombits(binary.LittleEndian.Uint64(data))
		return len(data) == numberSize && !math.IsNaN(value) && !math.IsInf(value, 0)
	case typeLiteral:
		return len(data) == 1 && data[0] <= 2
	case typeString:
		_, valid := UvarintPayload(data)
		return valid
	}
	return false
}

func BenchmarkContainerTraversal(b *testing.B) {
	for _, count := range []int{16, 4096} {
		for _, shape := range []string{"int64", "uint64", "float64", "literal", "string", "mixed"} {
			types, payloads := make([]byte, count), make([][]byte, count)
			for i := range types {
				tp := map[string]byte{"int64": typeInt64, "uint64": typeUint64, "float64": typeFloat64, "literal": typeLiteral, "string": typeString}[shape]
				if shape == "mixed" {
					tp = []byte{typeInt64, typeUint64, typeFloat64, typeLiteral, typeString}[i%5]
				}
				types[i] = tp
				switch tp {
				case typeLiteral:
					payloads[i] = []byte{1}
				case typeString:
					payloads[i] = []byte{1, 'x'}
				default:
					payloads[i] = make([]byte, 8)
					binary.LittleEndian.PutUint64(payloads[i], math.Float64bits(1))
				}
			}
			data := testScalarArray(types, payloads)
			for _, stored := range []bool{false, true} {
				b.Run(fmt.Sprintf("elements=%d/%s/stored=%v", count, shape, stored), func(b *testing.B) {
					if !Container(typeArray, data, benchmarkScalar) {
						b.Fatal("invalid ordinary fixture")
					}
					valid, depthExceeded := StoredContainer(typeArray, data, benchmarkScalar, uint64(len(data))*4)
					if !valid || depthExceeded {
						b.Fatal("invalid canonical fixture")
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if stored {
							valid, _ = StoredContainer(typeArray, data, benchmarkScalar, uint64(len(data))*4)
						} else {
							valid = Container(typeArray, data, benchmarkScalar)
						}
						if !valid {
							b.Fatal("fixture rejected")
						}
					}
				})
			}
		}
	}
}

func testWrapArray(child []byte, copies int, alias bool) []byte {
	header := headerSize + copies*valEntrySize
	payloadCopies := copies
	if alias {
		payloadCopies = 1
	}
	data := make([]byte, header+payloadCopies*len(child))
	binary.LittleEndian.PutUint32(data, uint32(copies))
	binary.LittleEndian.PutUint32(data[4:], uint32(len(data)))
	for i := 0; i < copies; i++ {
		offset := header
		if !alias {
			offset += i * len(child)
		}
		entry := headerSize + i*valEntrySize
		data[entry] = typeArray
		binary.LittleEndian.PutUint32(data[entry+1:], uint32(offset))
		copy(data[offset:], child)
	}
	return data
}

func TestContainerBoundsAliasedDescendantWork(t *testing.T) {
	data := testLiteralArray()
	for i := 0; i < 18; i++ {
		data = testWrapArray(data, 2, true)
	}
	visits := 0
	valid := Container(typeArray, data, func(byte, []byte) bool { visits++; return true })
	if valid || visits > len(data) {
		t.Fatalf("aliased descendants accepted=%v scalar visits=%d encoded bytes=%d", valid, visits, len(data))
	}
	// Distinct encoded children remain valid; no time-based assertion is used.
	data = testLiteralArray()
	for i := 0; i < 8; i++ {
		data = testWrapArray(data, 2, false)
	}
	visits = 0
	if !Container(typeArray, data, func(byte, []byte) bool { visits++; return true }) || visits != 512 {
		t.Fatalf("canonical tree: scalar visits=%d", visits)
	}
}

func TestContainerBoundsNesting(t *testing.T) {
	data := testLiteralArray()
	for i := 1; i < 100; i++ {
		data = testWrapArray(data, 1, false)
	}
	accept := func(byte, []byte) bool { return true }
	if !Container(typeArray, data, accept) {
		t.Fatal("100 container levels must remain valid")
	}
	data = testWrapArray(data, 1, false)
	visits := 0
	if Container(typeArray, data, func(byte, []byte) bool { visits++; return true }) || visits != 0 {
		t.Fatalf("over-depth chain was traversed: visits=%d", visits)
	}
}

func TestStoredContainerSharesBoundsAndWork(t *testing.T) {
	accept := func(byte, []byte) bool { return true }
	data := testLiteralArray()
	valid, depthExceeded := StoredContainer(typeArray, data, accept, 3)
	if !valid || depthExceeded {
		t.Fatal("root plus two inline entries must fit the exact work limit")
	}
	valid, depthExceeded = StoredContainer(typeArray, data, accept, 2)
	if valid || depthExceeded {
		t.Fatal("stored entry budget must reject before exceeding its limit")
	}
	data = testWrapArray(data, 1, false)
	valid, _ = StoredContainer(typeArray, data, accept, 4)
	if valid {
		t.Fatal("descendants must share, not reset, the stored work budget")
	}
	valid, _ = StoredContainer(typeArray, data, accept, 5)
	if !valid {
		t.Fatal("canonical distinct child fits the exact shared budget")
	}
	integer := make([]byte, headerSize+valEntrySize+numberSize)
	binary.LittleEndian.PutUint32(integer, 1)
	binary.LittleEndian.PutUint32(integer[docSizeOff:], uint32(len(integer)))
	integer[headerSize] = typeInt64
	binary.LittleEndian.PutUint32(integer[headerSize+valTypeSize:], headerSize+valEntrySize)
	valid, _ = StoredContainer(typeArray, integer, accept, 2)
	if valid {
		t.Fatal("non-inline scalar must charge its own stored work unit")
	}
	valid, _ = StoredContainer(typeArray, integer, accept, 3)
	if !valid {
		t.Fatal("non-inline scalar fits the exact stored work budget")
	}
	for i := 1; i < 100; i++ {
		data = testWrapArray(data, 1, false)
	}
	valid, depthExceeded = StoredContainer(typeArray, data, accept, ^uint64(0))
	if valid || !depthExceeded {
		t.Fatal("stored traversal must retain the distinct over-depth result")
	}
	data = testLiteralArray()
	for i := 0; i < 18; i++ {
		data = testWrapArray(data, 2, true)
	}
	visits := 0
	valid, _ = StoredContainer(typeArray, data, func(byte, []byte) bool { visits++; return true }, ^uint64(0))
	if valid || visits > len(data) {
		t.Fatalf("stored alias accepted=%v scalar visits=%d bytes=%d", valid, visits, len(data))
	}
}

func TestStoredContainerNumericEncodingEquivalent(t *testing.T) {
	nested := testScalarArray([]byte{typeInt64, typeUint64, typeFloat64, typeString}, [][]byte{make([]byte, 8), make([]byte, 8), make([]byte, 8), {1, 'x'}})
	data := testScalarArray([]byte{typeLiteral, typeArray, typeString}, [][]byte{{1}, nested, {1, 'y'}})
	check := func(d []byte, limit uint64) {
		t.Helper()
		want, wantDepth := StoredContainer(typeArray, d, benchmarkScalar, limit)
		got, gotDepth := StoredContainerWithNumericEncoding(typeArray, d, benchmarkScalar, limit)
		if got != want || gotDepth != wantDepth {
			t.Fatalf("limit=%d data=%x got=%v/%v want=%v/%v", limit, d, got, gotDepth, want, wantDepth)
		}
	}
	for i := range data {
		for _, value := range []byte{0, 1, 2, typeInt64, typeUint64, typeFloat64, 0xfd, 0xff} {
			d := append([]byte(nil), data...)
			d[i] = value
			for _, limit := range []uint64{0, 1, 20, 4096} {
				check(d, limit)
			}
		}
	}
	for _, value := range []float64{0, math.Copysign(0, -1), math.SmallestNonzeroFloat64, math.MaxFloat64, math.Inf(1), math.Inf(-1), math.NaN()} {
		payload := make([]byte, 8)
		binary.LittleEndian.PutUint64(payload, math.Float64bits(value))
		check(testScalarArray([]byte{typeFloat64}, [][]byte{payload}), 4096)
	}
	check(testWrapArray(nested, 2, true), 4096)
	deep := nested
	for i := 0; i < maxContainerDepth; i++ {
		deep = testWrapArray(deep, 1, false)
	}
	check(deep, 1<<20)

	ints := testScalarArray([]byte{typeInt64}, [][]byte{make([]byte, 8)})
	if ok, _ := StoredContainer(typeArray, ints, func(byte, []byte) bool { return false }, 4096); ok {
		t.Fatal("generic stored callback was bypassed")
	}
	if ok, _ := StoredContainerWithNumericEncoding(typeArray, ints, func(byte, []byte) bool { t.Fatal("numeric callback"); return false }, 4096); !ok {
		t.Fatal("valid integer rejected")
	}
	mutated := testScalarArray([]byte{typeString, typeInt64}, [][]byte{{1, 'x'}, make([]byte, 8)})
	visits := 0
	ok, _ := StoredContainerWithNumericEncoding(typeArray, mutated, func(tp byte, data []byte) bool {
		visits++
		mutated[headerSize+valEntrySize] = 0xfd
		return benchmarkScalar(tp, data)
	}, 4096)
	if ok || visits != 1 {
		t.Fatalf("later corruption bypassed: ok=%v visits=%d", ok, visits)
	}
}

func TestCanonicalStringRunsMatchBudgetedWalk(t *testing.T) {
	encode := func(payload []byte) []byte {
		return append(binary.AppendUvarint(nil, uint64(len(payload))), payload...)
	}
	short, unicode := encode([]byte("x")), encode([]byte("é"))
	long := encode(bytes.Repeat([]byte("z"), 130))
	nested := testScalarArray([]byte{typeString, typeString}, [][]byte{unicode, short})
	array := testScalarArray(
		[]byte{typeLiteral, typeString, typeString, typeString, typeInt64, typeArray, typeString},
		[][]byte{{1}, short, long, unicode, make([]byte, 8), nested, short},
	)
	const keys = 3
	valueStart := headerSize + keys*(keyEntrySize+valEntrySize) + keys
	object := make([]byte, valueStart+len(short)+len(long)+len(unicode))
	binary.LittleEndian.PutUint32(object, keys)
	binary.LittleEndian.PutUint32(object[docSizeOff:], uint32(len(object)))
	valueOffset := valueStart
	for i, value := range [][]byte{short, long, unicode} {
		key := headerSize + i*keyEntrySize
		binary.LittleEndian.PutUint32(object[key:], uint32(valueStart-keys+i))
		binary.LittleEndian.PutUint16(object[key+keyOriginOff:], 1)
		object[valueStart-keys+i] = byte('a' + i)
		entry := headerSize + keys*keyEntrySize + i*valEntrySize
		object[entry] = typeString
		binary.LittleEndian.PutUint32(object[entry+valTypeSize:], uint32(valueOffset))
		copy(object[valueOffset:], value)
		valueOffset += len(value)
	}
	predicate := func(tp byte, data []byte) bool {
		if tp == typeString {
			payload, ok := UvarintPayload(data)
			return ok && utf8.Valid(payload)
		}
		return benchmarkScalar(tp, data)
	}
	for _, fixture := range []struct {
		name string
		tp   byte
		data []byte
	}{{"array", typeArray, array}, {"object", typeObject, object}} {
		t.Run(fixture.name, func(t *testing.T) {
			check := func(data []byte, limit uint64) {
				t.Helper()
				want, wantDepth := StoredContainerWithNumericEncoding(fixture.tp, data, predicate, limit)
				got, gotDepth := StoredContainerWithCanonicalStrings(fixture.tp, data, predicate, limit)
				if got != want || gotDepth != wantDepth {
					t.Fatalf("limit=%d got=%v/%v want=%v/%v", limit, got, gotDepth, want, wantDepth)
				}
			}
			valid, depth := StoredContainerWithCanonicalStrings(fixture.tp, fixture.data, predicate, uint64(len(fixture.data))*4)
			if !valid || depth {
				t.Fatal("valid fixture rejected")
			}
			for limit := uint64(0); limit < 32; limit++ {
				check(fixture.data, limit)
			}
			for i := range fixture.data {
				for _, value := range []byte{0, 1, 0x03, 0x0c, 0x7f, 0x80, 0xa9, 0xc3, 0xfd, 0xff} {
					mutated := bytes.Clone(fixture.data)
					mutated[i] = value
					for _, limit := range []uint64{0, 3, 9, 16, uint64(len(mutated)) * 4} {
						check(mutated, limit)
					}
				}
			}
		})
	}
	deep := array
	for i := 0; i < maxContainerDepth; i++ {
		deep = testWrapArray(deep, 1, false)
	}
	want, wantDepth := StoredContainerWithNumericEncoding(typeArray, deep, predicate, uint64(len(deep))*4)
	got, gotDepth := StoredContainerWithCanonicalStrings(typeArray, deep, predicate, uint64(len(deep))*4)
	if got != want || gotDepth != wantDepth || !gotDepth {
		t.Fatalf("depth got=%v/%v want=%v/%v", got, gotDepth, want, wantDepth)
	}
	for _, invalid := range [][]byte{{0xff}, {0xc0, 0xaf}, {0xed, 0xa0, 0x80}, {0xf4, 0x90, 0x80, 0x80}, {0xc2}} {
		for _, run := range []bool{false, true} {
			types, values := []byte{typeString}, [][]byte{encode(invalid)}
			if run {
				types, values = append(types, typeString), append(values, short)
			}
			data := testScalarArray(types, values)
			valid, _ := StoredContainerWithCanonicalStrings(typeArray, data, predicate, uint64(len(data))*4)
			if valid {
				t.Fatalf("invalid UTF-8 accepted in run=%v: %x", run, invalid)
			}
		}
	}
}

func TestCanonicalStringRunsFlushBeforeOtherCallbacks(t *testing.T) {
	for _, repair := range []bool{false, true} {
		for limit := uint64(0); limit <= 12; limit++ {
			run := func(specialized bool) (bool, bool, int) {
				data := testScalarArray([]byte{typeString, typeString, typeLiteral, typeInt64}, [][]byte{{1, 'a'}, {1, 'b'}, {1}, make([]byte, 8)})
				later := headerSize + 3*valEntrySize
				if repair {
					data[later] = 0xfd
				}
				visits := 0
				predicate := func(tp byte, value []byte) bool {
					if tp == typeString {
						payload, ok := UvarintPayload(value)
						return ok && utf8.Valid(payload)
					}
					if tp == typeLiteral {
						visits++
						if repair {
							data[later] = typeInt64
						} else {
							data[later] = 0xfd
						}
					}
					return benchmarkScalar(tp, value)
				}
				var valid, depth bool
				if specialized {
					valid, depth = StoredContainerWithCanonicalStrings(typeArray, data, predicate, limit)
				} else {
					valid, depth = StoredContainerWithNumericEncoding(typeArray, data, predicate, limit)
				}
				return valid, depth, visits
			}
			want, wantDepth, wantVisits := run(false)
			got, gotDepth, gotVisits := run(true)
			if got != want || gotDepth != wantDepth || gotVisits != wantVisits {
				t.Fatalf("repair=%v limit=%d got=%v/%v/%d want=%v/%v/%d", repair, limit, got, gotDepth, gotVisits, want, wantDepth, wantVisits)
			}
		}
	}
}

func TestCanonicalStringRunsRespectByteBudget(t *testing.T) {
	data := testScalarArray([]byte{typeString, typeString, typeLiteral, typeInt64}, [][]byte{{1, 'a'}, {1, 'b'}, {1}, make([]byte, 8)})
	for _, budget := range []uint64{uint64(headerSize + 4*valEntrySize + 3), uint64(len(data))} {
		run := func(specialized bool) (bool, bool, int, uint64, uint64) {
			remaining, work := budget, uint64(len(data))*4
			visits := 0
			predicate := func(tp byte, value []byte) bool {
				if tp == typeString {
					payload, ok := UvarintPayload(value)
					return ok && utf8.Valid(payload)
				}
				visits++
				return benchmarkScalar(tp, value)
			}
			var depth bool
			valid := container(typeArray, data, predicate, 1, &remaining, &work, &depth, true, specialized)
			return valid, depth, visits, remaining, work
		}
		want, wantDepth, wantVisits, wantBytes, wantWork := run(false)
		got, gotDepth, gotVisits, gotBytes, gotWork := run(true)
		if got != want || gotDepth != wantDepth || gotVisits != wantVisits || got && (gotBytes != wantBytes || gotWork != wantWork) {
			t.Fatalf("budget=%d got=%v/%v/%d remaining=%d/%d want=%v/%v/%d remaining=%d/%d", budget, got, gotDepth, gotVisits, gotBytes, gotWork, want, wantDepth, wantVisits, wantBytes, wantWork)
		}
	}
}

func TestCanonicalStringRunsRespectPayloadBoundaries(t *testing.T) {
	predicate := func(tp byte, value []byte) bool {
		if tp == typeString {
			payload, ok := UvarintPayload(value)
			return ok && utf8.Valid(payload)
		}
		return benchmarkScalar(tp, value)
	}
	// A non-UTF-8 byte between valid, nonoverlapping payloads is padding and
	// must not be interpreted as part of either string.
	contiguous := testScalarArray([]byte{typeString, typeString}, [][]byte{{1, 'a'}, {1, 'b'}})
	secondEntry := headerSize + valEntrySize
	secondOffset := int(binary.LittleEndian.Uint32(contiguous[secondEntry+valTypeSize:]))
	withGap := make([]byte, len(contiguous)+1)
	copy(withGap, contiguous[:secondOffset])
	withGap[secondOffset] = 0xff
	copy(withGap[secondOffset+1:], contiguous[secondOffset:])
	binary.LittleEndian.PutUint32(withGap[docSizeOff:], uint32(len(withGap)))
	binary.LittleEndian.PutUint32(withGap[secondEntry+valTypeSize:], uint32(secondOffset+1))
	valid, _ := StoredContainerWithCanonicalStrings(typeArray, withGap, predicate, uint64(len(withGap))*4)
	if !valid {
		t.Fatal("valid string payloads separated by invalid padding rejected")
	}
	// The two payloads would form one valid rune if joined. Their ASCII length
	// prefixes separate them, so each invalid payload must still be rejected.
	split := testScalarArray([]byte{typeString, typeString, typeLiteral}, [][]byte{{1, 0xc3}, {1, 0xa9}, {1}})
	visits := 0
	observing := func(tp byte, value []byte) bool {
		if tp == typeLiteral {
			visits++
		}
		return predicate(tp, value)
	}
	valid, _ = StoredContainerWithCanonicalStrings(typeArray, split, observing, uint64(len(split))*4)
	if valid || visits != 0 {
		t.Fatalf("split invalid payloads valid=%v later callbacks=%d", valid, visits)
	}
}

func TestStoredNumericRunsPreserveBudgetsAndCallbacks(t *testing.T) {
	// Three numeric entries precede the callback and two follow it. The
	// complete walk costs 13 work units: one container, six entries and six
	// non-inline scalars.
	fixture := testScalarArray(
		[]byte{typeInt64, typeUint64, typeFloat64, typeString, typeInt64, typeUint64},
		[][]byte{make([]byte, 8), make([]byte, 8), make([]byte, 8), {1, 'x'}, make([]byte, 8), make([]byte, 8)},
	)
	type observation struct {
		valid, depth bool
		visits       int
	}
	run := func(change string, limit uint64, specialized bool) observation {
		data := bytes.Clone(fixture)
		later := headerSize + 4*valEntrySize
		if change == "repair" {
			data[later] = 0xfd
		}
		visits := 0
		predicate := func(tp byte, value []byte) bool {
			if tp == typeString {
				visits++
				switch change {
				case "corrupt":
					data[later] = 0xfd
				case "repair":
					data[later] = typeInt64
				}
			}
			return benchmarkScalar(tp, value)
		}
		var valid, depth bool
		if specialized {
			valid, depth = StoredContainerWithNumericEncoding(typeArray, data, predicate, limit)
		} else {
			valid, depth = StoredContainer(typeArray, data, predicate, limit)
		}
		return observation{valid, depth, visits}
	}
	for _, change := range []string{"none", "corrupt", "repair"} {
		for limit := uint64(0); limit <= 14; limit++ {
			want := run(change, limit, false)
			got := run(change, limit, true)
			if got != want {
				t.Fatalf("change=%s limit=%d got=%+v want=%+v", change, limit, got, want)
			}
		}
	}
	for _, tc := range []struct {
		change string
		limit  uint64
		want   observation
	}{
		{"none", 9, observation{false, false, 0}},
		{"none", 10, observation{false, false, 0}},
		{"none", 11, observation{false, false, 1}},
		{"none", 13, observation{true, false, 1}},
		{"corrupt", 13, observation{false, false, 1}},
		{"repair", 13, observation{true, false, 1}},
	} {
		if got := run(tc.change, tc.limit, true); got != tc.want {
			t.Fatalf("change=%s limit=%d got=%+v want=%+v", tc.change, tc.limit, got, tc.want)
		}
	}
}

func TestStoredNumericRunsRejectMalformedEntries(t *testing.T) {
	for _, change := range []string{"valid", "nonfinite", "overlap", "truncated", "unknown"} {
		t.Run(change, func(t *testing.T) {
			data := testScalarArray(
				[]byte{typeInt64, typeUint64, typeFloat64, typeInt64, typeUint64},
				[][]byte{make([]byte, 8), make([]byte, 8), make([]byte, 8), make([]byte, 8), make([]byte, 8)},
			)
			entry := func(i int) []byte { return data[headerSize+i*valEntrySize:] }
			switch change {
			case "nonfinite":
				offset := binary.LittleEndian.Uint32(entry(2)[valTypeSize:])
				binary.LittleEndian.PutUint64(data[offset:], math.Float64bits(math.Inf(1)))
			case "overlap":
				copy(entry(3)[valTypeSize:valEntrySize], entry(2)[valTypeSize:valEntrySize])
			case "truncated":
				binary.LittleEndian.PutUint32(entry(4)[valTypeSize:], uint32(len(data)-numberSize+1))
			case "unknown":
				entry(3)[0] = 0xfd
			}
			want, wantDepth := StoredContainer(typeArray, data, benchmarkScalar, 100)
			got, gotDepth := StoredContainerWithNumericEncoding(typeArray, data, benchmarkScalar, 100)
			if want != (change == "valid") || got != want || gotDepth != wantDepth {
				t.Fatalf("got=%v/%v want=%v/%v", got, gotDepth, want, wantDepth)
			}
		})
	}
}
