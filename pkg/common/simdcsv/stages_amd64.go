//+build !appengine
//+build !noasm
//+build gc

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
	"github.com/klauspost/cpuid"
	"log"
)

// Skeleton assembly routines
//
//go:noescape
func stage1_preprocess()

//go:noescape
func partialLoad()

//go:noescape
func stage2_parse()

// SupportedCPU will return whether the CPU is supported.
func SupportedCPU() bool {
	const want = cpuid.AVX2
	return cpuid.CPU.Features&want == want
}

//go:noescape
func stage1_preprocess_buffer(buf []byte, separatorChar uint64, input1 *stage1Input, output1 *stage1Output, postProc *[]uint64, offset uint64, masks []uint64, masksOffset uint64) (processed, masksWritten uint64)

//go:noescape
func stage1_preprocess_test(input *stage1Input, output *stage1Output)

func stage1PreprocessBuffer(buf []byte, separatorChar, quoted uint64) ([]uint64, []uint64, uint64) {

	return stage1PreprocessBufferEx(buf, separatorChar, quoted, nil, nil)
}

func stage1PreprocessBufferEx(buf []byte, separatorChar, quoted uint64, masks *[]uint64, postProc *[]uint64) ([]uint64, []uint64, uint64) {

	if postProc == nil {
		_postProc := make([]uint64, 0, 128)
		postProc = &_postProc
	}

	if masks == nil {
		_masks := allocMasks(buf)
		masks = &_masks
	}

	processed, masksOffset := uint64(0), uint64(0)
	inputStage1, outputStage1 := stage1Input{}, stage1Output{}
	inputStage1.quoted = quoted
	for {
		processed, masksOffset = stage1_preprocess_buffer(buf, separatorChar, &inputStage1, &outputStage1, postProc, processed, *masks, masksOffset)

		if processed >= uint64(len(buf)) {
			break
		}
		if masksOffset >= uint64(len(*masks)) {
			break
		}

		// Check if we need to grow the slice for keeping track of the lines to post process
		if len(*postProc) >= cap(*postProc)/2 {
			_postProc := make([]uint64, len(*postProc), cap(*postProc)*2)
			copy(_postProc, (*postProc)[:])
			postProc = &_postProc
		}
	}

	return (*masks)[:masksOffset], *postProc, inputStage1.quoted
}

//go:noescape
func _stage2_parse_masks(buf []byte, masks []uint64, lastCharIsDelimiter uint64, rows []uint64, columns []string, input2 *inputStage2, offset uint64, output2 *outputAsm) (processed, masksRead uint64)

func stage2_parse_masks(buf []byte, masks []uint64, rows []uint64, columns []string, delimiterChar uint64, input *inputStage2, offset uint64, output *outputAsm) (processed, masksRead uint64) {

	lastCharIsDelimiter := uint64(0)
	if len(buf) > 0 && (buf[len(buf)-1] == byte(delimiterChar) || buf[len(buf)-1] == byte(0x0d)) {
		lastCharIsDelimiter = 1
	}

	processed, masksRead = _stage2_parse_masks(buf, masks, lastCharIsDelimiter, rows, columns, input, offset, output)
	return
}

// Perform CSV parsing on a buffer
//
// `records` may be passed in, if non-nil it will be reused
// and grown accordingly
func stage2ParseBuffer(buf []byte, masks []uint64, delimiterChar uint64, records *[][]string) ([][]string, bool) {

	r, _, _, parseError := stage2ParseBufferEx(buf, masks, delimiterChar, records, nil, nil)
	return r, parseError
}

// Same as above, but allow reuse of `rows` and `columns` slices as well
func stage2ParseBufferEx(buf []byte, masks []uint64, delimiterChar uint64, records *[][]string, rows *[]uint64, columns *[]string) ([][]string, []uint64, []string, bool) {

	errorOut := func() ([][]string, []uint64, []string, bool) {
		*columns = (*columns)[:0]
		*rows = (*rows)[:0]
		return *records, *rows, *columns, true
	}

	if rows == nil {
		_rows := make([]uint64, 1024) // do not reserve less than 128
		rows = &_rows
	}
	if columns == nil {
		_columns := make([]string, 10240)
		columns = &_columns
	}

	// for repeat calls the actual lengths may have been reduced, so set arrays to maximum size
	*rows = (*rows)[:cap(*rows)]
	*columns = (*columns)[:cap(*columns)]

	if records == nil {
		_records := make([][]string, 0, 1024)
		records = &_records
	}

	*records = (*records)[:0]

	inputStage2, outputStage2 := newInputStage2(), outputAsm{}

	offset, masksOffset := uint64(0), uint64(0)
	for {
		processed, masksRead := stage2_parse_masks(buf, masks[masksOffset:], *rows, *columns, delimiterChar, &inputStage2, offset, &outputStage2)
		if inputStage2.errorOffset != 0 {
			return errorOut()
		}
		if int(processed) >= len(buf) {
			break
		}

		// Sanity checks
		if offset == processed {
			log.Fatalf("failed to process anything")
		} else if masksOffset+masksRead > uint64(len(masks)) {
			log.Fatalf("processed beyond end of masks buffer")
		}
		offset = processed
		masksOffset += masksRead

		// Check whether we need to double columns slice capacity
		if outputStage2.index/2 >= cap(*columns)/2 {
			_columns := make([]string, cap(*columns)*2)
			copy(_columns, (*columns)[:outputStage2.index/2])
			columns = &_columns
		}

		// Check whether we need to double rows slice capacity
		if outputStage2.line >= cap(*rows)/2 {
			_rows := make([]uint64, cap(*rows)*2)
			copy(_rows, (*rows)[:outputStage2.line])
			rows = &_rows
		}
	}

	// Is the final quoted field not closed?
	if inputStage2.quoted != 0 {
		return errorOut()
	}

	//if outputStage2.index >= 2 {
	//	// Sanity check -- we must not point beyond the end of the buffer
	//	if peek(uintptr(unsafe.Pointer(&(*columns)[0])), uint64(outputStage2.index-2)*8) != 0 &&
	//		peek(uintptr(unsafe.Pointer(&(*columns)[0])), uint64(outputStage2.index-2)*8) - uint64(uintptr(unsafe.Pointer(&buf[0]))) +
	//			peek(uintptr(unsafe.Pointer(&(*columns)[0])), uint64(outputStage2.index-1)*8) > uint64(len(buf)) {
	//		log.Fatalf("ERROR: Pointing past end of buffer")
	//	}
	//}

	*columns = (*columns)[:(outputStage2.index)/2]
	*rows = (*rows)[:outputStage2.line]

	for i := 0; i < len(*rows); i += 2 {
		*records = append(*records, (*columns)[(*rows)[i]:(*rows)[i]+(*rows)[i+1]])
	}

	return *records, *rows, *columns, false
}

// Same as above, but allow reuse of `rows` and `columns` slices as well
func stage2ParseBufferExStreaming(buf []byte, masks []uint64, delimiterChar uint64, inputStage2 *inputStage2, outputStage2 *outputAsm, rows *[]uint64, columns *[]string) ([]uint64, []string, bool) {

	errorOut := func() ([]uint64, []string, bool) {
		*columns = (*columns)[:0]
		*rows = (*rows)[:0]
		return *rows, *columns, true
	}

	if rows == nil {
		_rows := make([]uint64, 1024) // do not reserve less than 128
		rows = &_rows
	}
	if columns == nil {
		_columns := make([]string, 10240)
		columns = &_columns
	}

	// for repeat calls the actual lengths may have been reduced, so set arrays to maximum size
	*rows = (*rows)[:cap(*rows)]
	*columns = (*columns)[:cap(*columns)]

	offset, masksOffset := uint64(0), uint64(0)
	for {
		processed, masksRead := stage2_parse_masks(buf, masks[masksOffset:], *rows, *columns, delimiterChar, inputStage2, offset, outputStage2)
		if inputStage2.errorOffset != 0 {
			return errorOut()
		}
		if int(processed) >= len(buf) {
			break
		}

		// sanity checks
		if offset == processed {
			log.Fatalf("failed to process anything")
		} else if masksOffset+masksRead > uint64(len(masks)) {
			log.Fatalf("processed beyond end of masks buffer")
		}
		offset = processed
		masksOffset += masksRead

		// check whether we need to double columns slice capacity
		if outputStage2.index/2 >= cap(*columns)*4/5 {
			_columns := make([]string, cap(*columns)*3/2)
			copy(_columns, (*columns)[:outputStage2.index/2])
			columns = &_columns
		}

		// check whether we need to double rows slice capacity
		if outputStage2.line >= cap(*rows)*4/5 {
			_rows := make([]uint64, cap(*rows)*3/2)
			copy(_rows, (*rows)[:outputStage2.line])
			rows = &_rows
		}
	}

	// Is the final quoted field not closed?
	if inputStage2.quoted != 0 {
		return errorOut()
	}

	return *rows, *columns, false
}

//go:noescape
func stage2_parse_test(input *inputStage2, offset uint64, output *outputStage2)
