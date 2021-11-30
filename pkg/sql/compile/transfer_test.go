package compile

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"github.com/matrixorigin/matrixone/pkg/sql/protocol"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"testing"
)

func TestScope(t *testing.T) {
	var buf bytes.Buffer
	cs := &Scope{
		Magic: CreateDatabase,
		DataSource: &Source{
			IsMerge: true,
			SchemaName: "db1",
			RelationName: "table1",
			RefCounts: []uint64{1, 23423, 543234},
			Attributes: []string{"col1", "col2", "col3"},
		},
		PreScopes: nil,
		NodeInfo: engine.Node{
			Id: "node id",
			Addr: "engine address",
		},
		Instructions: nil,
	}
	transScope := Transfer(cs)
	err := protocol.EncodeScope(transScope, &buf)
	require.NoError(t, err)
	ResultScope, _, err := protocol.DecodeScope(buf.Bytes())
	require.NoError(t, err)
	// Magic
	if ResultScope.Magic != CreateDatabase{
		t.Errorf("Decode Scope type failed.")
		return
	}
	// DataSource
	if ResultScope.DataSource.IsMerge != true {
		t.Errorf("Decode Scope DataSource failed.")
		return
	}
	if ResultScope.DataSource.SchemaName != "db1" {
		t.Errorf("Decode Scope DataSource failed.")
		return
	}
	if ResultScope.DataSource.RelationName != "table1" {
		t.Errorf("Decode Scope DataSource failed.")
		return
	}
	for i, v := range ResultScope.DataSource.RefCounts {
		if v != cs.DataSource.RefCounts[i] {
			t.Errorf("Decode Scope DataSource failed.")
			return
		}
	}
	for i, v := range ResultScope.DataSource.Attributes {
		if v != cs.DataSource.Attributes[i] {
			t.Errorf("Decode Scope DataSource failed.")
			return
		}
	}
	// NodeInfo
	if ResultScope.NodeInfo.Id != "node id" {
		t.Errorf("Decode Scope NodeInfo failed.")
		return
	}
	if ResultScope.NodeInfo.Addr != "engine address" {
		t.Errorf("Decode Scope NodeInfo failed.")
		return
	}
}