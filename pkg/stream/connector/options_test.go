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

package moconnector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeStmtOpts(t *testing.T) {
	empty := StmtOpts{}
	o, err := MakeStmtOpts(context.Background(), nil)
	assert.NoError(t, err)
	assert.Equal(t, empty, o)

	noTypeOpts := map[string]string{
		"value": "json",
	}
	_, err = MakeStmtOpts(context.Background(), noTypeOpts)
	assert.Error(t, err)

	notSupportedOpts := map[string]string{
		"abc": "abc",
	}
	_, err = MakeStmtOpts(context.Background(), notSupportedOpts)
	assert.Error(t, err)

	notSupportedOpts = map[string]string{
		"type": "kafka",
		"abc":  "abc",
	}
	_, err = MakeStmtOpts(context.Background(), notSupportedOpts)
	assert.Error(t, err)

	invalidValueOptList := []map[string]string{
		{"type": ""},
		{"type": "my"},
		{"type": "kafka", "bootstrap.servers": "localhost"},
		{"type": "kafka", "value": "a"},
	}
	for _, opt := range invalidValueOptList {
		_, err = MakeStmtOpts(context.Background(), opt)
		assert.Error(t, err)
	}

	lackOpts := map[string]string{"type": "kafka", "partition": "1"}
	_, err = MakeStmtOpts(context.Background(), lackOpts)
	assert.Error(t, err)

	lackOpts = map[string]string{"type": "kafka1", "partition": "1"}
	_, err = MakeStmtOpts(context.Background(), lackOpts)
	assert.Error(t, err)

	okOpts := map[string]string{
		"type":              "kafka",
		"bootstrap.servers": "localhost:9092",
		"topic":             "t1",
		"value":             "json",
	}
	o, err = MakeStmtOpts(context.Background(), okOpts)
	assert.NoError(t, err)
	assert.Equal(t, o, StmtOpts(okOpts))
}
