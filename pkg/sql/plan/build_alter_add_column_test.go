package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDropColumnWithIndex(t *testing.T) {
	var def TableDef
	def.Indexes = []*IndexDef{
		{IndexName: "idx",
			IndexAlgo:  "fulltext",
			TableExist: true,
			Unique:     false,
			Parts:      []string{"body", "title"},
		},
	}

	err := handleDropColumnWithIndex(context.TODO(), "body", &def)
	require.Nil(t, err)
	require.Equal(t, 1, len(def.Indexes[0].Parts))
	require.Equal(t, "title", def.Indexes[0].Parts[0])
}
