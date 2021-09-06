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
	"log"
	"math/bits"
	"reflect"
	"unsafe"
)

type stage1Input struct {
	quoteMaskIn          uint64
	separatorMaskIn      uint64
	carriageReturnMaskIn uint64
	quoteMaskInNext      uint64
	quoted               uint64
	newlineMaskIn        uint64
	newlineMaskInNext    uint64
}

type stage1Output struct {
	quoteMaskOut          uint64
	separatorMaskOut      uint64
	carriageReturnMaskOut uint64
	needsPostProcessing   uint64
}

func preprocessMasks(input *stage1Input, output *stage1Output) {

	const clearMask = 0xfffffffffffffffe

	separatorMaskIn := input.separatorMaskIn
	carriageReturnMaskIn := input.carriageReturnMaskIn
	quoteMaskIn := input.quoteMaskIn

	separatorPos := bits.TrailingZeros64(separatorMaskIn)
	carriageReturnPos := bits.TrailingZeros64(carriageReturnMaskIn)
	quotePos := bits.TrailingZeros64(quoteMaskIn)

	output.quoteMaskOut = quoteMaskIn                   // copy quote mask to output
	output.separatorMaskOut = separatorMaskIn           // copy separator mask to output
	output.carriageReturnMaskOut = carriageReturnMaskIn // copy carriage return mask to output
	output.needsPostProcessing = 0                      // flag to indicate whether post-processing is need for these masks

	for {
		if quotePos < separatorPos && quotePos < carriageReturnPos {

			if input.quoted != 0 && quotePos == 63 && input.quoteMaskInNext&1 == 1 { // last bit of quote mask and first bit of next quote mask set?
				// clear out both active bit and ...
				quoteMaskIn &= clearMask << quotePos
				output.quoteMaskOut &= ^(uint64(1) << quotePos) // mask out quote
				output.needsPostProcessing = 1                  // post-processing is required for double quotes
				// first bit of next quote mask
				input.quoteMaskInNext &= ^uint64(1)
			} else if input.quoted != 0 && quoteMaskIn&(1<<(quotePos+1)) != 0 { // next quote bit is also set (so two adjacent bits) ?
				// clear out both active bit and subsequent bit
				quoteMaskIn &= clearMask << (quotePos + 1)
				output.quoteMaskOut &= ^(uint64(3) << quotePos) // mask out two quotes
				output.needsPostProcessing = 1                  // post-processing is required for double quotes
			} else {
				input.quoted = ^input.quoted

				quoteMaskIn &= clearMask << quotePos
			}

			quotePos = bits.TrailingZeros64(quoteMaskIn)

		} else if separatorPos < quotePos && separatorPos < carriageReturnPos {

			if input.quoted != 0 {
				output.separatorMaskOut &= ^(uint64(1) << separatorPos) // mask out separator bit in quoted field
			}

			separatorMaskIn &= clearMask << separatorPos
			separatorPos = bits.TrailingZeros64(separatorMaskIn)

		} else if carriageReturnPos < quotePos && carriageReturnPos < separatorPos {

			if input.quoted != 0 {
				output.carriageReturnMaskOut &= ^(uint64(1) << carriageReturnPos) // mask out carriage return bit in quoted field
				output.needsPostProcessing = 1                                    // post-processing is required for carriage returns in quoted fields
			} else {
				if carriageReturnPos == 63 { // special handling for last position of mask
					if input.newlineMaskInNext&1 == 0 {
						output.carriageReturnMaskOut &= ^(uint64(1) << carriageReturnPos) // mask out carriage return for replacement without following newline
					}
				} else {
					if input.newlineMaskIn&(uint64(1)<<(carriageReturnPos+1)) == 0 {
						output.carriageReturnMaskOut &= ^(uint64(1) << carriageReturnPos) // mask out carriage return bit in quoted field
					}
				}
			}

			carriageReturnMaskIn &= clearMask << carriageReturnPos
			carriageReturnPos = bits.TrailingZeros64(carriageReturnMaskIn)

		} else {
			// we must be done
			break
		}
	}

	return
}

type postProcRow struct {
	start int
	end   int
}

//
// Determine which rows and columns need post processing
// This is needed to replace both "" to " as well as
// \r\n to \n for specific fields
func getPostProcRows(buf []byte, postProc []uint64, simdrecords [][]string) []postProcRow {

	// TODO: Crude implementation, make more refined/granular

	sliceptr := func(slc []byte) uintptr {
		return (*reflect.SliceHeader)(unsafe.Pointer(&slc)).Data
	}
	stringptr := func(s string) uintptr {
		return (*reflect.StringHeader)(unsafe.Pointer(&s)).Data
	}

	ppRows := make([]postProcRow, 0, 128)

	row, pbuf := 0, sliceptr(buf)
	for ipp, pp := range postProc {

		if ipp < len(postProc)-1 && pp == postProc[ipp+1] {
			continue // if offset occurs multiple times, process only last one
		}

		// find start row to process
		for row < len(simdrecords) && uint64(stringptr(simdrecords[row][0])-pbuf) < pp {
			row++
		}

		ppr := postProcRow{}
		if row > 0 {
			ppr.start = row - 1
		}

		// find end row to process
		for row < len(simdrecords) && uint64(stringptr(simdrecords[row][0])-pbuf) < pp+64 {
			row++
		}
		ppr.end = row

		ppRows = append(ppRows, ppr)
	}

	if len(ppRows) <= 1 {
		return ppRows
	}

	// merge overlapping ranges into a single range
	ppRowsMerged := make([]postProcRow, 0, len(ppRows))

	start, end := ppRows[0].start, ppRows[0].end
	for _, pp := range ppRows[1:] {
		if end < pp.start {
			ppRowsMerged = append(ppRowsMerged, postProcRow{start, end})
			start, end = pp.start, pp.end
		} else {
			end = pp.end
		}
	}
	ppRowsMerged = append(ppRowsMerged, postProcRow{start, end})

	return ppRowsMerged
}

func diffBitmask(diff1, diff2 string) (diff string) {
	if len(diff1) != len(diff2) {
		log.Fatalf("sizes don't match")
	}

	for i := range diff1 {
		if diff1[i] != diff2[i] {
			diff += "^"
		} else {
			diff += " "
		}
	}

	return diff1 + "\n" + diff2 + "\n" + diff + "\n"
}
