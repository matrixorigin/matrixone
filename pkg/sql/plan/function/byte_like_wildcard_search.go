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

package function

import "unsafe"

// Direct verification is substantially cheaper per operation than a modular
// butterfly. Convolution is only useful when its candidate-by-segment upper
// bound is superlinear by a meaningful factor relative to reading the value
// and segment once. In particular, one or a few legal alignments always stay
// on the allocation-free direct path, regardless of segment size.
const (
	byteLikeConvolutionRelativeWorkFactor uint64 = 64
	byteLikeMaxMismatchTerm               uint64 = 636284160
)

const (
	byteLikeMaxNTTLength              = 1 << 27
	byteLikeCancellationCheckInterval = 1 << 16
)

// Both primes support power-of-two transforms through 2^27. For encoded bytes
// p,x in [1,256], each mismatch term p*x*(p-x)^2 is at most byteLikeMaxMismatchTerm.
// Their product is larger than 64 MiB times that bound, so zero under both
// moduli is an exact result, not a probabilistic fingerprint.
var byteLikeNTTModuli = [...]struct {
	modulus       uint64
	primitiveRoot uint64
}{
	{modulus: 2013265921, primitiveRoot: 31},
	{modulus: 2281701377, primitiveRoot: 3},
}

const byteLikeMaxExactConvolutionSegment = uint64(64 << 20)

func (compiled *compiledByteLikePattern) findSegmentByConvolution(
	start, end int,
	value []byte,
	from, limit int,
) (int, bool, error) {
	segmentLength := end - start
	candidateCount := limit - from - segmentLength + 1
	if segmentLength <= 0 || candidateCount <= 0 ||
		uint64(segmentLength) > byteLikeMaxExactConvolutionSegment {
		return -1, false, nil
	}

	// Limit each transform to at most one segment-length of candidate
	// alignments. Failed blocks consume disjoint value ranges; the one final
	// block per segment adds only O(segmentLength) work. Across a complete
	// LIKE match this bounds convolution work by O((value+pattern) log pattern)
	// instead of repeating a full remaining-value transform for every '%'.
	for blockFrom, remaining := from, candidateCount; remaining > 0; {
		blockCandidates := min(segmentLength, remaining)
		blockLimit := blockFrom + blockCandidates + segmentLength - 1
		matchAt, used, err := compiled.findSegmentConvolutionBlock(
			start, end, value, blockFrom, blockLimit)
		if err != nil || !used || matchAt >= 0 {
			return matchAt, used, err
		}
		blockFrom += blockCandidates
		remaining -= blockCandidates
	}
	return -1, true, nil
}

func (compiled *compiledByteLikePattern) findSegmentConvolutionBlock(
	start, end int,
	value []byte,
	from, limit int,
) (int, bool, error) {
	if err := compiled.byteLikeCancellationError(); err != nil {
		return -1, true, err
	}
	segmentLength := end - start
	valueLength := limit - from
	candidateCount := valueLength - segmentLength + 1
	convolutionLength := valueLength + segmentLength - 1
	transformLength := 1
	for transformLength < convolutionLength {
		if transformLength >= byteLikeMaxNTTLength {
			return -1, false, nil
		}
		transformLength <<= 1
	}

	uint32Bytes := transformLength * 4
	requiredScratch := uint32Bytes*3 + candidateCount
	if cap(compiled.convolutionScratch) < requiredScratch {
		scratch, err := compiled.mp.Grow(compiled.convolutionScratch, requiredScratch, true)
		if err != nil {
			return -1, true, err
		}
		compiled.convolutionScratch = scratch
	}
	compiled.convolutionScratch = compiled.convolutionScratch[:requiredScratch]
	a := byteLikeUint32Scratch(compiled.convolutionScratch[:uint32Bytes], transformLength)
	b := byteLikeUint32Scratch(compiled.convolutionScratch[uint32Bytes:uint32Bytes*2], transformLength)
	sum := byteLikeUint32Scratch(compiled.convolutionScratch[uint32Bytes*2:uint32Bytes*3], transformLength)
	zeroFirst := compiled.convolutionScratch[uint32Bytes*3:]
	clear(zeroFirst)

	for modulusIndex, parameters := range byteLikeNTTModuli {
		clear(sum)
		for term := 0; term < 3; term++ {
			clear(a)
			clear(b)
			if err := compiled.fillByteLikeConvolutionTerm(
				a, b, start, end, value[from:limit], term, parameters.modulus); err != nil {
				return -1, true, err
			}
			if err := compiled.byteLikeNTT(a, false, parameters.modulus, parameters.primitiveRoot); err != nil {
				return -1, true, err
			}
			if err := compiled.byteLikeNTT(b, false, parameters.modulus, parameters.primitiveRoot); err != nil {
				return -1, true, err
			}
			coefficient := uint64(1)
			if term == 1 {
				coefficient = parameters.modulus - 2
			}
			for i := range sum {
				if i&(byteLikeCancellationCheckInterval-1) == 0 {
					if err := compiled.byteLikeCancellationError(); err != nil {
						return -1, true, err
					}
				}
				product := uint64(a[i]) * uint64(b[i]) % parameters.modulus
				sum[i] = uint32((uint64(sum[i]) + coefficient*product) % parameters.modulus)
			}
		}
		if err := compiled.byteLikeNTT(sum, true, parameters.modulus, parameters.primitiveRoot); err != nil {
			return -1, true, err
		}

		anyZero := false
		for candidate := 0; candidate < candidateCount; candidate++ {
			if candidate&(byteLikeCancellationCheckInterval-1) == 0 {
				if err := compiled.byteLikeCancellationError(); err != nil {
					return -1, true, err
				}
			}
			mismatch := sum[segmentLength-1+candidate]
			if modulusIndex == 0 {
				if mismatch == 0 {
					zeroFirst[candidate] = 1
					anyZero = true
				}
			} else if zeroFirst[candidate] != 0 && mismatch == 0 {
				return from + candidate, true, nil
			}
		}
		if modulusIndex == 0 && !anyZero {
			return -1, true, nil
		}
	}
	return -1, true, nil
}

func byteLikeUint32Scratch(storage []byte, length int) []uint32 {
	return unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(storage))), length)
}

func (compiled *compiledByteLikePattern) fillByteLikeConvolutionTerm(
	patternValues, textValues []uint32,
	start, end int,
	value []byte,
	term int,
	modulus uint64,
) error {
	patternPower := 3 - term
	textPower := term + 1
	for patternAt := start; patternAt < end; patternAt++ {
		if (patternAt-start)&(byteLikeCancellationCheckInterval-1) == 0 {
			if err := compiled.byteLikeCancellationError(); err != nil {
				return err
			}
		}
		if compiled.kinds[patternAt] != byteLikeLiteral {
			continue
		}
		encoded := uint64(compiled.literals[patternAt]) + 1
		patternValues[end-1-patternAt] = uint32(byteLikeSmallPower(encoded, patternPower) % modulus)
	}
	for valueAt, literal := range value {
		if valueAt&(byteLikeCancellationCheckInterval-1) == 0 {
			if err := compiled.byteLikeCancellationError(); err != nil {
				return err
			}
		}
		encoded := uint64(literal) + 1
		textValues[valueAt] = uint32(byteLikeSmallPower(encoded, textPower) % modulus)
	}
	return nil
}

func byteLikeSmallPower(value uint64, power int) uint64 {
	result := uint64(1)
	for i := 0; i < power; i++ {
		result *= value
	}
	return result
}

func (compiled *compiledByteLikePattern) byteLikeNTT(
	values []uint32,
	inverse bool,
	modulus, primitiveRoot uint64,
) error {
	for i, j := 1, 0; i < len(values); i++ {
		if i&(byteLikeCancellationCheckInterval-1) == 0 {
			if err := compiled.byteLikeCancellationError(); err != nil {
				return err
			}
		}
		bit := len(values) >> 1
		for ; j&bit != 0; bit >>= 1 {
			j ^= bit
		}
		j ^= bit
		if i < j {
			values[i], values[j] = values[j], values[i]
		}
	}

	for width := 2; width <= len(values); width <<= 1 {
		root := byteLikeModPow(primitiveRoot, (modulus-1)/uint64(width), modulus)
		if inverse {
			root = byteLikeModPow(root, modulus-2, modulus)
		}
		half := width >> 1
		for block := 0; block < len(values); block += width {
			factor := uint64(1)
			for offset := 0; offset < half; offset++ {
				if (block+offset)&(byteLikeCancellationCheckInterval-1) == 0 {
					if err := compiled.byteLikeCancellationError(); err != nil {
						return err
					}
				}
				left := uint64(values[block+offset])
				right := uint64(values[block+offset+half]) * factor % modulus
				sum := left + right
				if sum >= modulus {
					sum -= modulus
				}
				difference := left + modulus - right
				if difference >= modulus {
					difference -= modulus
				}
				values[block+offset] = uint32(sum)
				values[block+offset+half] = uint32(difference)
				factor = factor * root % modulus
			}
		}
	}
	if inverse {
		inverseLength := byteLikeModPow(uint64(len(values)), modulus-2, modulus)
		for i := range values {
			if i&(byteLikeCancellationCheckInterval-1) == 0 {
				if err := compiled.byteLikeCancellationError(); err != nil {
					return err
				}
			}
			values[i] = uint32(uint64(values[i]) * inverseLength % modulus)
		}
	}
	return nil
}

func (compiled *compiledByteLikePattern) byteLikeCancellationError() error {
	if compiled.ctx == nil {
		return nil
	}
	return compiled.ctx.Err()
}

func byteLikeModPow(base, exponent, modulus uint64) uint64 {
	result := uint64(1)
	for exponent != 0 {
		if exponent&1 != 0 {
			result = result * base % modulus
		}
		base = base * base % modulus
		exponent >>= 1
	}
	return result
}
