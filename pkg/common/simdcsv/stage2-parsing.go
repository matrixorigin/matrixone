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
	_ "fmt"
	"math/bits"
	"unsafe"
)

func stage2Parse(buffer []byte, delimiter, separator, quote rune,
	f func(input *inputStage2, offset uint64, output *outputStage2)) ([]uint64, []uint64, uint64) {

	separatorMasks := getBitMasks([]byte(buffer), byte(separator))
	delimiterMasks := getBitMasks([]byte(buffer), byte(delimiter))
	quoteMasks := getBitMasks([]byte(buffer), byte(quote))

	//fmt.Printf(" separator: %064b\n", bits.Reverse64(separatorMasks[0]))
	//fmt.Printf(" delimiter: %064b\n", bits.Reverse64(delimiterMasks[0]))
	//fmt.Printf("     quote: %064b\n", bits.Reverse64(quoteMasks[0]))

	columns, rows := [128]uint64{}, [128]uint64{}
	columns[0] = 0
	offset := uint64(0)

	input := newInputStage2()
	output := outputStage2{columns: &columns, rows: &rows}

	for maskIndex := 0; maskIndex < len(separatorMasks); maskIndex++ {
		input.separatorMask = separatorMasks[maskIndex]
		input.delimiterMask = delimiterMasks[maskIndex]
		input.quoteMask = quoteMasks[maskIndex]

		f(&input, offset, &output)
		offset += 0x40
	}

	if input.quoted != 0 {
		input.errorOffset = uint64(len(buffer))
	}

	return columns[:output.index], rows[:output.line], input.errorOffset
}

//
// Make sure references to struct from assembly stay in sync
//
type inputStage2 struct {
	separatorMask            uint64
	delimiterMask            uint64
	quoteMask                uint64
	quoted                   uint64
	lastSeparatorOrDelimiter uint64
	lastClosingQuote         uint64
	errorOffset              uint64
	base                     unsafe.Pointer // #define INPUT_BASE 0x38
}

// Create new inputStage2
func newInputStage2() (input inputStage2) {
	input = inputStage2{lastSeparatorOrDelimiter: ^uint64(0)}
	return
}

//
// Make sure references to struct from assembly stay in sync
//
type outputStage2 struct {
	columns   *[128]uint64 // #define COLUMNS_BASE 0x0
	index     int          // #define INDEX_OFFSET 0x8
	rows      *[128]uint64 // #define ROWS_BASE    0x10
	line      int          // #define LINE_OFFSET  0x18
	strData   uint64
	strLen    uint64
	indexPrev uint64
}

// Equivalent for invoking from Assembly
type outputAsm struct {
	columns   unsafe.Pointer // #define COLUMNS_BASE 0x0
	index     int            // #define INDEX_OFFSET 0x8
	rows      unsafe.Pointer // #define ROWS_BASE    0x10
	line      int            // #define LINE_OFFSET  0x18
	strData   uint64
	strLen    uint64
	indexPrev uint64
}

func stage2ParseMasks(input *inputStage2, offset uint64, output *outputStage2) {

	const clearMask = 0xfffffffffffffffe

	separatorPos := bits.TrailingZeros64(input.separatorMask)
	delimiterPos := bits.TrailingZeros64(input.delimiterMask)
	quotePos := bits.TrailingZeros64(input.quoteMask)

	for {
		if separatorPos < delimiterPos && separatorPos < quotePos {

			if input.quoted == 0 {
				// verify that last closing quote is immediately followed by either a separator or delimiter
				if input.lastClosingQuote > 0 &&
					input.lastClosingQuote+1 != uint64(separatorPos)+offset {
					if input.errorOffset == 0 {
						input.errorOffset = uint64(separatorPos) + offset // mark first error position
					}
				}
				input.lastClosingQuote = 0

				output.columns[output.index] = uint64(uintptr(input.base)) + output.strData // pointer to start of element
				output.index++
				output.columns[output.index] = (-output.strLen + uint64(separatorPos) + offset) - output.strData // size of element
				output.index++
				output.strData = uint64(separatorPos) + offset + 1 // start of next element
				output.strLen = 0

				input.lastSeparatorOrDelimiter = uint64(separatorPos) + offset
			}

			input.separatorMask &= clearMask << separatorPos
			separatorPos = bits.TrailingZeros64(input.separatorMask)

		} else if delimiterPos < separatorPos && delimiterPos < quotePos {

			if input.quoted == 0 {
				// verify that last closing quote is immediately followed by either a separator or delimiter
				if input.lastClosingQuote > 0 &&
					input.lastClosingQuote+1 != uint64(delimiterPos)+offset {
					if input.errorOffset == 0 {
						input.errorOffset = uint64(delimiterPos) + offset // mark first error position
					}
				}
				input.lastClosingQuote = 0

				// for delimiters, we may end exactly on a separator (without a delimiter following),
				// this leads to a length of zero for the string, so we nil out the pointer value
				// (to avoid potentially pointing exactly at the first available byte after the buffer)
				if (-output.strLen+uint64(delimiterPos)+offset)-output.strData == 0 {
					output.columns[output.index] = 0 // pointer to start of element
				} else {
					output.columns[output.index] = uint64(uintptr(input.base)) + output.strData // pointer to start of element
				}
				output.index++
				output.columns[output.index] = (-output.strLen + uint64(delimiterPos) + offset) - output.strData // size of element element
				output.index++
				output.strData = uint64(delimiterPos) + offset + 1 // start of next element
				output.strLen = 0

				if uint64(output.index)/2-output.indexPrev == 1 && // we just have a line with a single element
					output.columns[output.index-1] == 0 { // and its length is zero (implying empty line)
					// prevent empty lines from being written
				} else {
					// write out start and length for a new row
					output.rows[output.line] = output.indexPrev // start of element
					output.line++
					output.rows[output.line] = uint64(output.index)/2 - output.indexPrev // length of element
					output.line++
				}

				output.indexPrev = uint64(output.index) / 2 // keep current index for next round

				input.lastSeparatorOrDelimiter = uint64(delimiterPos) + offset
			}

			input.delimiterMask &= clearMask << delimiterPos
			delimiterPos = bits.TrailingZeros64(input.delimiterMask)

		} else if quotePos < separatorPos && quotePos < delimiterPos {

			if input.quoted == 0 {
				// check that this opening quote is preceded by either a separator or delimiter
				if input.lastSeparatorOrDelimiter+1 != uint64(quotePos)+offset {
					if input.errorOffset == 0 {
						input.errorOffset = uint64(quotePos) + offset
					}
				}
				// we advance slice pointer by one (effectively skipping over starting quote)
				output.strData += 1
			} else {
				// we reduce the length by one (effectively making sure closing quote is excluded)
				output.strLen += 1
				input.lastClosingQuote = uint64(quotePos) + offset // record position of last closing quote
			}

			input.quoted = ^input.quoted

			input.quoteMask &= clearMask << quotePos
			quotePos = bits.TrailingZeros64(input.quoteMask)

		} else {
			// we must be done
			break
		}
	}
}

func getBitMasks(buf []byte, cmp byte) (masks []uint64) {

	if len(buf)%64 != 0 {
		// NB code is not used during normal usage
		panic("Input strings should be a multiple of 64")
	}

	masks = make([]uint64, 0)

	for i := 0; i < len(buf); i += 64 {
		mask := uint64(0)
		for b, c := range buf[i : i+64] {
			if c == cmp {
				mask = mask | (1 << b)
			}
		}
		masks = append(masks, mask)
	}
	return
}
