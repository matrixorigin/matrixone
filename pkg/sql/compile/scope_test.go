// Copyright 2021 Matrix Origin
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

/*
func TestScopeSerialization(t *testing.T) {
	var sourceScopes = generateScopeCases(t)

	for i, sourceScope := range sourceScopes {
		data, errEncode := encodeScope(sourceScope)
		require.NoError(t, errEncode)
		targetScope, errDecode := decodeScope(data)
		require.NoError(t, errDecode)
		fmt.Println(fmt.Sprintf("number is %d", i))
		fmt.Println(fmt.Sprintf("sourceScope : %v", sourceScope))
		fmt.Println(fmt.Sprintf("targetScope : %v", targetScope))

		// require.Equal(t, sourceScope, targetScope, fmt.Sprintf("index is %d", i))
	}

}

func generateScopeCases(t *testing.T) []*Scope {
	// getScope method generate and return the scope of a SQL string.
	getScope := func(t1 *testing.T, sql string) *Scope {
		proc := testutil.NewProcess()
		e := memEngine.NewTestEngine()
		opt := plan2.NewBaseOptimizer(e.(*memEngine.MemEngine))
		stmts, err := mysql.Parse(sql)
		require.NoError(t1, err)
		qry, err := opt.Optimize(stmts[0])
		require.NoError(t1, err)
		c := New("test", sql, "", context.Background(), e, proc, nil)
		err = c.Compile(&plan.Plan{Plan: &plan.Plan_Query{Query: qry}}, nil, func(a any, batch *batch.Batch) error {
			return nil
		})
		require.NoError(t1, err)
		return c.scope
	}

	testCases := []string{
		"select 1",
		"select * from R",
		"select count(*) from R",
		"select * from R limit 2, 1",
		"select * from R left join S on R.uid = S.uid",
	}
	result := make([]*Scope, len(testCases))
	for i, sql := range testCases {
		result[i] = getScope(t, sql)
	}
	return result
}
*/
