// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestScope_indexTableBuild(t *testing.T) {
	indexTableDef := &plan.TableDef{
		TblId:  0,
		Name:   "__mo_index_secondary_01932ed7-a7e1-7c40-b914-25053a71a825",
		Hidden: false,
		Cols: []*plan.ColDef{
			{
				Name:   "doc_id",
				Hidden: false,
				Alg:    plan.CompressType_Lz4,
				Typ: plan.Type{
					Id:          22,
					NotNullable: false,
					AutoIncr:    false,
					Width:       32,
					Scale:       -1,
				},
				Default: &plan.Default{},
				NotNull: false,
				Primary: false,
			},
			{
				Name:   "pos",
				Hidden: false,
				Alg:    plan.CompressType_Lz4,
				Typ: plan.Type{
					Id:          22,
					NotNullable: false,
					AutoIncr:    false,
					Width:       32,
					Scale:       -1,
				},
				Default: &plan.Default{},
				NotNull: false,
				Primary: false,
			},
			{
				Name:   "word",
				Hidden: false,
				Alg:    plan.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       65535,
					Scale:       0,
				},
				Default: &plan.Default{},
				NotNull: false,
				Primary: false,
			},
			{
				Name:   "__mo_fake_pk_col",
				Hidden: true,
				Alg:    plan.CompressType_Lz4,
				Typ: plan.Type{
					Id:          28,
					NotNullable: false,
					AutoIncr:    true,
					Width:       0,
					Scale:       0,
				},
				Default: &plan.Default{},
				NotNull: true,
				Primary: true,
			},
		},
		Pkey: &plan.PrimaryKeyDef{
			PkeyColId:   0,
			PkeyColName: "__mo_fake_pk_col",
			Names:       []string{"__mo_fake_pk_col"},
		},
		Defs: []*plan.TableDef_DefType{
			{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{
							{
								Key:   "relkind",
								Value: "i",
							},
						},
					},
				},
			},
		},
	}

	convey.Convey("indexTableBuild FaultTolerance1", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		proc.ReplaceTopCtx(ctx)

		eng := mock_frontend.NewMockEngine(ctrl)
		mockDbMeta := mock_frontend.NewMockDatabase(ctrl)
		mockDbMeta.EXPECT().RelationExists(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
		mockDbMeta.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Return(moerr.NewInternalErrorNoCtx("test"))

		c := NewCompile("test", "test", "create fulltext index ftidx on t1 (b)", "", "", eng, proc, nil, false, nil, time.Now())

		err := indexTableBuild(c, indexTableDef, mockDbMeta)
		assert.Error(t, err)
	})

	convey.Convey("indexTableBuild FaultTolerance2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		proc.ReplaceTopCtx(ctx)

		eng := mock_frontend.NewMockEngine(ctrl)
		mockDbMeta := mock_frontend.NewMockDatabase(ctrl)
		mockDbMeta.EXPECT().RelationExists(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
		mockDbMeta.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

		maybeAutoIncrement := gostub.Stub(&maybeCreateAutoIncrement, func(
			ctx context.Context,
			sid string,
			db engine.Database,
			def *plan.TableDef,
			txnOp client.TxnOperator,
			nameResolver func() string) error {
			return moerr.NewInternalErrorNoCtx("test")
		})
		defer maybeAutoIncrement.Reset()

		c := NewCompile("test", "test", "create fulltext index ftidx on t1 (b)", "", "", eng, proc, nil, false, nil, time.Now())

		err := indexTableBuild(c, indexTableDef, mockDbMeta)
		assert.Error(t, err)
	})

}
