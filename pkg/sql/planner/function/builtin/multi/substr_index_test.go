// Copyright 2022 Matrix Origin
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

package multi

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSubstrIndedxInt64(t *testing.T) {
	testCases := []arg{
		{
			info: "substring_index('www.mysql.com', '.' , 0 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarInt64(0, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("", 1),
		},

		{
			info: "substring_index('www.mysql.com', '.' , 1 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarInt64(1, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("www", 1),
		},

		{
			info: "substring_index('www.mysql.com', '.' , 2 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("www.mysql", 1),
		},

		{
			info: "substring_index('www.mysql.com', '.' , 3 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarInt64(3, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("www.mysql.com", 1),
		},

		{
			info: "substring_index('www.mysql.com', '.' , -3 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarInt64(-3, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("www.mysql.com", 1),
		},

		{
			info: "substring_index('www.mysql.com', '.' , -2 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarInt64(-2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("mysql.com", 1),
		},

		{
			info: "substring_index('www.mysql.com', '.' , -1 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarInt64(-1, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("com", 1),
		},

		{
			info: "substring_index('www.mysql.com', '' , 1 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar("", 1),
				testutil.MakeScalarInt64(1, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("", 1),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			gotV, err := SubStrIndex(tc.vs, tc.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, vector.MustStrCol(tc.expect), vector.MustStrCol(gotV))
		})
	}
}

func TestSubstrIndedxUint64(t *testing.T) {
	testCases := []arg{
		{
			info: "substring_index('www.mysql.com', '.' , 0 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarUint64(0, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("", 1),
		},

		{
			info: "substring_index('www.mysql.com', '.' , 1 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarUint64(1, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("www", 1),
		},

		{
			info: "substring_index('www.mysql.com', '.' , 2 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarUint64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("www.mysql", 1),
		},

		{
			info: "substring_index('www.mysql.com', '.' , 3 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarUint64(3, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("www.mysql.com", 1),
		},

		{
			info: "substring_index('www.mysql.com', '.' , 18446744073709551613)", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarUint64(18446744073709551613, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("www.mysql.com", 1),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			gotV, err := SubStrIndex(tc.vs, tc.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, vector.MustStrCol(tc.expect), vector.MustStrCol(gotV))
		})
	}
}

func TestSubstrIndedxFloat64(t *testing.T) {
	testCases := []arg{
		{
			info: "substring_index('www.mysql.com', '.' , 0 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarFloat64(0, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("", 1),
		},

		{
			info: "substring_index('www.mysql.com', '.' , 1 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarFloat64(1.0, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("www", 1),
		},

		{
			info: "substring_index('www.mysql.com', '.' , 2 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarFloat64(2.0, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("www.mysql", 1),
		},

		{
			info: "substring_index('www.mysql.com', '.' , 3 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarFloat64(3.0, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("www.mysql.com", 1),
		},

		{
			info: "substring_index('www.mysql.com', '.' , -3 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarFloat64(-3.0, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("www.mysql.com", 1),
		},

		{
			info: "substring_index('www.mysql.com', '.' , -2 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarFloat64(-2.0, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("mysql.com", 1),
		},

		{
			info: "substring_index('www.mysql.com', '.' , -1 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarFloat64(-1.0, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("com", 1),
		},

		{
			info: "substring_index('www.mysql.com', '' , 1 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar("", 1),
				testutil.MakeScalarFloat64(1.0, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("", 1),
		},

		{
			info: "substring_index('www.mysql.com', '' , 1 )", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("www.mysql.com", 1),
				testutil.MakeScalarVarchar(".", 1),
				testutil.MakeScalarFloat64(85938593859330403.0, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("www.mysql.com", 1),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			gotV, err := SubStrIndex(tc.vs, tc.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, vector.MustStrCol(tc.expect), vector.MustStrCol(gotV))
		})
	}
}
