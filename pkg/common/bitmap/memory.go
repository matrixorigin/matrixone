// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package bitmap

// Allocated is retained backing capacity. Size reports the logical word length.
func (n *Bitmap) Allocated() int {
	if n == nil {
		return 0
	}
	return cap(n.data) * 8
}

// GrowthAllocation is the complete new storage allocated by TryExpandWithSize.
// Existing words remain live until copying finishes. A zero result reuses data.
func (n *Bitmap) GrowthAllocation(size int) int64 {
	if size <= 0 || n != nil && n.len >= int64(size) {
		return 0
	}
	words := int64((uint64(size) + 63) / 64)
	if n != nil && words <= int64(cap(n.data)) {
		return 0
	}
	return words * 8
}
