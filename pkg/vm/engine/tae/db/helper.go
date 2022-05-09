package db

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

func MakeSegmentScopes(entries ...*catalog.SegmentEntry) (scopes []common.ID) {
	for _, entry := range entries {
		scopes = append(scopes, *entry.AsCommonID())
	}
	return
}

func MakeBlockScopes(entries ...*catalog.BlockEntry) (scopes []common.ID) {
	for _, entry := range entries {
		scopes = append(scopes, *entry.AsCommonID())
	}
	return
}
