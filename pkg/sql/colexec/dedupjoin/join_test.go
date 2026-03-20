package dedupjoin

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCheckDuplicateKeysInRange_NoConflict(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	rel := mock_frontend.NewMockRelation(ctrl)
	keyVec := testutil.MakeInt32Vector([]int32{1}, nil)
	keyBat := batch.NewWithSize(1)
	keyBat.Vecs[0] = keyVec
	keyBat.SetRowCount(1)

	rel.EXPECT().PrimaryKeysMayBeUpserted(gomock.Any(), gomock.Any(), gomock.Any(), keyBat, int32(0)).Return(false, nil)

	rowStr, err := checkDuplicateKeysInRange(
		proc,
		rel,
		types.BuildTS(1, 0),
		types.BuildTS(2, 0),
		keyBat,
		keyVec,
		"id",
		[]plan.Type{{Id: int32(types.T_int32)}},
	)
	require.NoError(t, err)
	require.Empty(t, rowStr)
}

func TestCheckDuplicateKeysInRange_ReturnsDuplicateRow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	rel := mock_frontend.NewMockRelation(ctrl)
	keyVec := testutil.MakeInt32Vector([]int32{1}, nil)
	keyBat := batch.NewWithSize(1)
	keyBat.Vecs[0] = keyVec
	keyBat.SetRowCount(1)

	rel.EXPECT().PrimaryKeysMayBeUpserted(gomock.Any(), gomock.Any(), gomock.Any(), keyBat, int32(0)).Return(true, nil)

	rowStr, err := checkDuplicateKeysInRange(
		proc,
		rel,
		types.BuildTS(1, 0),
		types.BuildTS(2, 0),
		keyBat,
		keyVec,
		"id",
		[]plan.Type{{Id: int32(types.T_int32)}},
	)
	require.NoError(t, err)
	require.Equal(t, "1", rowStr)
}
