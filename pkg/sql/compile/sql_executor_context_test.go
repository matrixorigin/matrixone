// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Test_panic(t *testing.T) {
	r := func() {
		err := recover()
		require.Equal(t, err, "not supported in internal sql executor")
	}

	c := &compilerContext{}

	func() {
		defer r()
		_ = c.CheckSubscriptionValid("", "", "")
	}()

	func() {
		defer r()
		_, _ = c.IsPublishing("")
	}()

	func() {
		defer r()
		c.SetQueryingSubscription(nil)
	}()

	func() {
		defer r()
		_, _ = c.ResolveUdf("", nil)
	}()

	func() {
		defer r()
		_, _ = c.ResolveAccountIds(nil)
	}()

	func() {
		defer r()
		_, _, _ = c.GetQueryResultMeta("")
	}()

	func() {
		defer r()
		c.SetBuildingAlterView(false, "", "")
	}()

	func() {
		defer r()
		_, _, _ = c.GetBuildingAlterView()
	}()
}

func TestCompilerContext_Database(t *testing.T) {
	ctrl := gomock.NewController(t)
	database := mock_frontend.NewMockDatabase(ctrl)
	database.EXPECT().GetDatabaseId(nil).Return("1")
	engine := mock_frontend.NewMockEngine(ctrl)
	engine.EXPECT().Database(nil, "", nil).Return(database, nil).Times(2)

	c := &compilerContext{
		proc:   &process.Process{},
		engine: engine,
	}

	exists := c.DatabaseExists("")
	require.Equal(t, exists, true)

	_, err := c.GetDatabaseId("")
	require.Nil(t, err)

	sql := c.GetRootSql()
	require.Equal(t, sql, "")
}
