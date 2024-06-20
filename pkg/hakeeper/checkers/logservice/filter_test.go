// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
)

func TestSelector(t *testing.T) {
	cases := []struct {
		shardInfo logservice.LogShardInfo
		stores    map[string]logservice.Locality
		required  logservice.Locality
		expected  string
	}{
		// empty locality
		{
			shardInfo: logservice.LogShardInfo{
				Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
			},
			stores: map[string]logservice.Locality{
				"a": {},
				"b": {},
				"c": {},
				"d": {},
			},
			expected: "d",
		},
		{
			shardInfo: logservice.LogShardInfo{
				Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
			},
			stores: map[string]logservice.Locality{
				"a": {},
				"b": {},
				"c": {},
			},
			expected: "",
		},
		{
			shardInfo: logservice.LogShardInfo{
				Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
			},
			stores: map[string]logservice.Locality{
				"a":     {},
				"b":     {},
				"c":     {},
				"e":     {},
				"hello": {},
				"d":     {},
			},
			expected: "d",
		},
		{
			shardInfo: logservice.LogShardInfo{
				Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
			},
			stores: map[string]logservice.Locality{
				"a":     {},
				"b":     {},
				"c":     {},
				"e":     {},
				"hello": {},
				"d":     {Value: map[string]string{"k1": "v1"}},
			},
			expected: "e",
		},

		// not empty locality
		{
			shardInfo: logservice.LogShardInfo{
				Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
			},
			stores: map[string]logservice.Locality{
				"a": {},
				"b": {},
				"c": {},
				"d": {Value: map[string]string{"k1": "v1"}},
			},
			required: logservice.Locality{
				Value: map[string]string{"k1": "v1"},
			},
			expected: "d",
		},

		{
			shardInfo: logservice.LogShardInfo{
				Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
			},
			stores: map[string]logservice.Locality{
				"a": {},
				"b": {},
				"c": {},
				"d": {Value: map[string]string{"k1": "v2"}},
			},
			required: logservice.Locality{
				Value: map[string]string{"k1": "v1"},
			},
			expected: "",
		},
	}

	for _, c := range cases {
		output := selectStore(c.shardInfo, c.stores, c.required)
		assert.Equal(t, c.expected, output)
	}
}

func TestFilterLocality(t *testing.T) {
	cases := []struct {
		store    logservice.Locality
		required logservice.Locality
		expect   bool
	}{
		{
			store: logservice.Locality{
				Value: map[string]string{
					"a": "a",
					"b": "b",
				},
			},
			required: logservice.Locality{
				Value: map[string]string{
					"a": "a",
				},
			},
			expect: true,
		},
		{
			store: logservice.Locality{
				Value: map[string]string{
					"a": "a",
					"b": "b",
				},
			},
			required: logservice.Locality{
				Value: map[string]string{
					"a": "a",
					"b": "a",
				},
			},
			expect: false,
		},
		{
			store: logservice.Locality{
				Value: map[string]string{
					"a": "a",
					"b": "b",
				},
			},
			required: logservice.Locality{
				Value: map[string]string{
					"a": "a",
					"b": "b",
					"c": "c",
				},
			},
			expect: false,
		},
	}

	for _, c := range cases {
		r := filterLocality(c.store, c.required)
		assert.Equal(t, c.expect, r)
	}
}
