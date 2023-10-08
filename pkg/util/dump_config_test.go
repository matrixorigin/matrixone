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

package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type abc struct {
	A string
	B string `user_setting:"advanced"`
}

type kkk struct {
	kk string `user_setting:"basic"`
	jj string
}

type def struct {
	FFF *abc  `user_setting:"advanced"`
	DDD []kkk `user_setting:"advanced"`
	ABC []abc `toml:"abc"`
	EEE [3]abc
	MMM map[string]string
	NNN map[string]abc
	PPP *abc
	QQQ *abc
	RRR map[string]string
}

func Test_flatten(t *testing.T) {
	cf1 := def{
		ABC: []abc{
			{"a", "a1"},
			{"b", "b1"},
		},
		EEE: [3]abc{
			{"a", "a1"},
			{"b", "b1"},
			{"c", "c1"},
		},
		MMM: map[string]string{
			"abc":  "ABC",
			"abcd": "ABCd",
		},
		NNN: map[string]abc{
			"abc":  {"a", "a1"},
			"abcd": {"a", "a1"},
		},
		PPP: &abc{"a", "a1"},
		FFF: &abc{"a", "a1"},
	}
	exp := newExporter()
	err := flatten(cf1, "", "", "", exp)
	assert.NoError(t, err)
	err = flatten(&cf1, "", "", "", exp)
	assert.NoError(t, err)

	k := kkk{
		"abc",
		"def",
	}
	err = flatten(k, "", "", "", exp)
	assert.NoError(t, err)
	exp.Print()
}
