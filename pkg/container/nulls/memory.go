// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package nulls

// Allocated counts retained bitmap backing capacity.
func Allocated(nsp *Nulls) int {
	if nsp == nil {
		return 0
	}
	return nsp.np.Allocated()
}

// GrowthAllocation predicts complete new storage while old data remains live.
func GrowthAllocation(nsp *Nulls, size int) int64 {
	if nsp == nil {
		return int64((uint64(max(size, 0))+63)/64) * 8
	}
	return nsp.np.GrowthAllocation(size)
}
