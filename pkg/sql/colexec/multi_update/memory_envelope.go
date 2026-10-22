// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package multi_update

import (
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// OutputEnvelope covers one metadata frame while its input is borrowed from
// D/W. Building a frame can retain bytes.Buffer old+new plus the output vector's
// complete new area. Eval instead reads one fresh vector area per Call.
func OutputEnvelope(unit, maxRowBytes int64, tableCount int) (int64, error) {
	if unit <= 0 || maxRowBytes < 0 || tableCount < 0 {
		return 0, fmt.Errorf("invalid import output envelope")
	}
	q := max(unit, maxRowBytes)
	if q > (math.MaxInt64-1<<20)/4 {
		return 0, fmt.Errorf("import output envelope overflow")
	}
	// Each projection's partial final block contributes one extra metadata
	// entry, in addition to rows/BlockMaxRows for full blocks. A complete flush
	// covers at most the target plus the crossing window, across all tables.
	blocks := 2*unit/objectio.BlockMaxRows + 4*int64(tableCount) + 2
	entry := int64(objectio.BlockInfoSize + objectio.ObjectStatsLen + 4*64)
	if blocks > math.MaxInt64/entry {
		return 0, fmt.Errorf("import metadata envelope overflow")
	}
	meta := blocks * entry
	if meta > (math.MaxInt64-1<<20)/4 {
		return 0, fmt.Errorf("import metadata envelope overflow")
	}
	return 4*max(q, meta) + (1 << 20), nil
}
