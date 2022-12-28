// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestTableID(t *testing.T) {
	type testCase struct {
		ta       api.TableID
		tb       api.TableID
		evaluate func(t require.TestingT, expected interface{}, actual interface{}, msgAndArgs ...interface{})
	}

	cases := []testCase{
		{
			ta:       newTable(1, 1, 1),
			tb:       newTable(1, 1, 1),
			evaluate: require.Equal,
		},
		{
			ta:       newTable(1, 1, 1),
			tb:       newTable(2, 1, 1),
			evaluate: require.NotEqual,
		},
		{
			ta:       newTable(1, 1, 1),
			tb:       newTable(1, 2, 1),
			evaluate: require.NotEqual,
		},
		{
			ta:       newTable(1, 1, 1),
			tb:       newTable(1, 1, 2),
			evaluate: require.NotEqual,
		},
	}

	for _, c := range cases {
		taID := TableID(c.ta.String())
		tbID := TableID(c.tb.String())
		c.evaluate(t, taID, tbID)
	}
}

func newTable(dbID, tbID, ptID uint64) api.TableID {
	return api.TableID{
		DbId:        dbID,
		TbId:        tbID,
		PartitionId: ptID,
	}
}

func newTimestamp(physical int64, logical uint32) timestamp.Timestamp {
	return timestamp.Timestamp{
		PhysicalTime: physical,
		LogicalTime:  logical,
	}
}
