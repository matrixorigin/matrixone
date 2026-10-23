// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package colexec

import (
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// InsertMetadataOutputEnvelope bounds a complete flush's fixed object metadata,
// independently of source-row width and of unrelated shared output-slot users.
// Both controlled Insert and MultiUpdate flush before crossing the common byte
// unit. A legal wider singleton contributes one block, not its raw payload.
// Each projection can contribute partial blocks; full blocks have 8192 rows
// and every physical row consumes at least one byte. ObjectStats count cannot
// exceed block count. The per-entry allowance covers both encoded varlena
// headers and the fixed BlockInfo/ObjectStats payloads.
func InsertMetadataOutputEnvelope(unit int64, tableCount int) (int64, error) {
	if unit <= 0 || tableCount < 0 {
		return 0, fmt.Errorf("invalid insert metadata envelope")
	}
	// Compute floor(2*unit/BlockMaxRows) without overflowing 2*unit.
	blocks := 2*(unit/objectio.BlockMaxRows) + 2*(unit%objectio.BlockMaxRows)/objectio.BlockMaxRows
	if int64(tableCount) > (math.MaxInt64-blocks-2)/4 {
		return 0, fmt.Errorf("insert metadata block count overflows")
	}
	blocks += 4*int64(tableCount) + 2
	entry := int64(objectio.BlockInfoSize + objectio.ObjectStatsLen + 4*64)
	// Keep the existing four-copy serializer/growth allowance and fixed
	// overhead. The data unit only derives item count; it is not itself a
	// metadata payload or a reason to copy a raw JSON singleton into I.
	if blocks > (math.MaxInt64-(1<<20))/(4*entry) {
		return 0, fmt.Errorf("insert metadata envelope overflows")
	}
	return 4*blocks*entry + (1 << 20), nil
}

// InsertMetadataFrameLimit is the protocol limit used before serialization and
// propagated to metadata consumers. A larger raw join/delete output lease must
// not enlarge this INSERT-only frame contract.
func InsertMetadataFrameLimit(unit int64, tableCount int) (int64, error) {
	output, err := InsertMetadataOutputEnvelope(unit, tableCount)
	return output / 4, err
}
