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

package metric

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnvOrDefaultBool(t *testing.T) {
	key := "MO_TEST"
	assert.Equal(t, envOrDefaultBool(key, 42), int32(42))
	for _, s := range []string{"1", "True", "T", "true"} {
		os.Setenv(key, s)
		assert.Equal(t, envOrDefaultBool(key, 42), int32(1))
	}
	for _, s := range []string{"0", "f", "false"} {
		os.Setenv(key, s)
		assert.Equal(t, envOrDefaultBool(key, 42), int32(0))
	}

	for _, s := range []string{"", "nope", "stop"} {
		os.Setenv(key, s)
		assert.Equal(t, envOrDefaultBool(key, 42), int32(42))
	}
}

func TestEnvOrDefaultInt(t *testing.T) {
	key := "MO_TEST"
	assert.Equal(t, envOrDefaultInt[int32](key, 42), int32(42))

	for _, i := range []int{1, 2, 3, 5} {
		os.Setenv(key, strconv.Itoa(i))
		assert.Equal(t, envOrDefaultInt[int32](key, 42), int32(i))
	}
	for _, s := range []string{"x", "02f1", "ffs", "0x12"} {
		os.Setenv(key, s)
		assert.Equal(t, envOrDefaultInt[int64](key, 42), int64(42))
	}
}
