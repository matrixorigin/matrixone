package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

func TestTableObjectStats(t *testing.T) {
	db := MockDBEntryWithAccInfo(0, 0)
	tbl := MockTableEntryWithDB(db, 1)
	_, detail := tbl.ObjectStats(common.PPL4, 0, 1, false)
	require.Equal(t, "DATA\n", detail.String())

	tbl.dataObjects.Set(MockObjEntryWithTbl(tbl, 10), true)
	_, detail = tbl.ObjectStats(common.PPL4, 0, 1, false)
	require.Equal(t, "DATA\n\n00000000-0000-0000-0000-000000000000_0\n    loaded:true, oSize:0B, cSzie:10B rows:1, zm: ZM(ANY)0[<nil>,<nil>]--\n", detail.String())

	tbl.tombstoneObjects.Set(MockTombstoneEntryWithTbl(tbl, 20), true)
	_, detail = tbl.ObjectStats(common.PPL4, 0, 1, true)
	require.Equal(t, "TOMBSTONES\n\n00000000-0000-0000-0000-000000000000_0\n    loaded:true, oSize:0B, cSzie:20B rows:1, zm: ZM(ANY)0[<nil>,<nil>]--\n", detail.String())
}
