// Copyright 2021 - 2023 Matrix Origin
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

package ctl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIdentifyParser(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		p := identifyParser("cn_1:k1:v1")
		require.NotNil(t, p)
		switch p.(type) {
		case *singleValue:
		default:
			t.Fatalf("wrong type")
		}
		label := p.parseLabel()
		require.Equal(t, "cn_1", label.uuid)
		require.Equal(t, "k1", label.key)
		require.Equal(t, []string{"v1"}, label.values)

		p = identifyParser("cn_2:k2:[v1,v_2,v3]")
		require.NotNil(t, p)
		switch p.(type) {
		case *multiValues:
		default:
			t.Fatalf("wrong type")
		}
		label = p.parseLabel()
		require.Equal(t, "cn_2", label.uuid)
		require.Equal(t, "k2", label.key)
		require.Equal(t, []string{"v1", "v_2", "v3"}, label.values)
	})

	t.Run("fails", func(t *testing.T) {
		cases := []string{
			"&&&",
			"aaa",
			"a:b",
			":a:b:c",
			"a::b:c",
			"a:b::c",
			"a: b:c",
			"a:b:c:",
			"a^:b:c",
			"a:b-1:c",
			"a:b:c-1",
			"a:b:[]",
			"a:b:[c:]",
			"a:b:[c,d:]",
			"a:b:[:]",
			"a:b:[c,]",
			"a:b:[c,d-1]",
			"a:b:[c,d]]",
			"a:b:[[c,d]",
		}
		for _, c := range cases {
			p := identifyParser(c)
			require.Nil(t, p)
		}
	})
}

func TestHandleSetLabel(t *testing.T) {
	// This case is tested in DebugUpdateCNLabel in clusterservice package.
}
