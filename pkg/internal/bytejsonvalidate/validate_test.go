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
)

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
				valid := container(typeArray, data, func(byte, []byte) bool { visits++; return true }, 1, &remaining, storedWork, nil)
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
