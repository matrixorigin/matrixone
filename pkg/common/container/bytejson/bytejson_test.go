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

package bytejson

import (
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestLiteral(t *testing.T) {
	j := []string{"true", "false", "null"}
	for _, x := range j {
		bj, err := ParseFromString(x)
		require.Nil(t, err)
		require.Equal(t, x, bj.String())
	}
}

func TestNumber(t *testing.T) {
	// generate max int64
	j := []string{
		"9223372036854775807",
		"-9223372036854775808",
		"1",
		"-1",
	}
	for _, x := range j {
		bj, err := ParseFromString(x)
		require.Nil(t, err)
		// transform string to int64
		now, err := strconv.ParseInt(x, 10, 64)
		require.Nil(t, err)
		require.Equal(t, now, bj.GetInt64())
	}

	// generate max uint64
	j = []string{
		"18446744073709551615",
		"0",
		"1",
	}
	for _, x := range j {
		bj, err := ParseFromString(x)
		require.Nil(t, err)
		// transform string to uint64
		now, err := strconv.ParseUint(x, 10, 64)
		require.Nil(t, err)
		require.Equal(t, now, bj.GetUint64())
	}

	//generate max float64
	j = []string{
		"1.7976931348623157e+308",
		"-1.7976931348623157e+308",
		"4.940656458412465441765687928682213723651e-324",
		"0.112131431",
		"1.13353411",
	}
	for _, x := range j {
		bj, err := ParseFromString(x)
		require.Nil(t, err)
		// transform string to float64
		now, err := strconv.ParseFloat(x, 64)
		require.Nil(t, err)
		require.Equal(t, now, bj.GetFloat64())
	}
}

func TestObject(t *testing.T) {
	j := []string{
		"{\"a\":\"1a\"}",
		"{\"a\": \"1\", \"b\": \"2\"}",
		"{\"a\": \"1\", \"b\": \"2\", \"c\": \"3\"}",
		"{\"result\":[[\"卫衣女\",\"80928.08611854467\"],[\"卫衣男\",\"74626.82297478059\"],[\"卫衣外套\",\"83628.47912479182\"],[\"卫衣套装\",\"62563.97733227019\"],[\"卫衣裙\",\"267198.7776653171\"],[\"卫衣裙女夏\",\"43747.754850631354\"],[\"卫衣连衣裙女\",\"279902.456837801\"],[\"卫衣套装女夏\",\"52571.576083414795\"],[\"卫衣女夏\",\"116244.16040484588\"],[\"卫衣oversize小众高街潮牌ins\",\"59134.02621045128\"]]}",
	}
	for _, x := range j {
		bj, err := ParseFromString(x)
		require.Nil(t, err)
		require.JSONEq(t, x, bj.String())
	}
}
func TestArray(t *testing.T) {
	j := []string{
		"[",
		"[{]",
		"[{}]",
		"[\"1\"]",
		"[\"1\", \"2\"]",
		"[\"1\", \"2\", \"3\"]",
		"[[\"卫衣女\",\"80928.08611854467\"],[\"卫衣男\",\"74626.82297478059\"],[\"卫衣外套\",\"83628.47912479182\"],[\"卫衣套装\",\"62563.97733227019\"],[\"卫衣裙\",\"267198.7776653171\"],[\"卫衣裙女夏\",\"43747.754850631354\"],[\"卫衣连衣裙女\",\"279902.456837801\"],[\"卫衣套装女夏\",\"52571.576083414795\"],[\"卫衣女夏\",\"116244.16040484588\"],[\"卫衣oversize小众高街潮牌ins\",\"59134.02621045128\"]]",
	}
	for i, x := range j {
		bj, err := ParseFromString(x)
		if i > 1 {
			require.Nil(t, err)
			require.JSONEq(t, x, bj.String())
		} else {
			require.NotNil(t, err)
		}
	}
}
