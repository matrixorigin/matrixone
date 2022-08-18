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

package testtxnengine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testBVT(t *testing.T, globPattern string) {
	files, err := filepath.Glob(globPattern)
	assert.Nil(t, err)
	assert.True(t, len(files) > 0)
	sort.Strings(files)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	env, err := newEnv(ctx)
	assert.Nil(t, err)
	defer func() {
		if err := env.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	tx := env.NewTx()
	defer func() {
		if err == nil {
			err = tx.Commit(ctx)
		}
	}()

	for _, file := range files {
		content, err := os.ReadFile(file)
		assert.Nil(t, err)

		// trim comment lines
		lines := bytes.Split(content, []byte("\n"))
		filtered := lines[:0]
		for _, line := range lines {
			if bytes.HasPrefix(line, []byte("--")) {
				continue
			}
			filtered = append(filtered, line)
		}
		content = bytes.Join(filtered, []byte("\n"))

		err = tx.Exec(ctx, file, string(content))
		assert.Nil(t, err)
	}

}

func TestTPCH(t *testing.T) {
	_ = testBVT
	t.Skip()
	testBVT(
		t,
		filepath.Join(
			"..", "..", "..", "..", "..",
			"test", "cases", "benchmark", "tpch", "*", "*",
		),
	)
}

func TestDML(t *testing.T) {
	t.Skip()
	for _, dir := range []string{
		"select",
		"insert",
		"update",
		"delete",
	} {
		testBVT(
			t,
			filepath.Join(
				"..", "..", "..", "..", "..",
				"test", "cases", "dml", dir, "*",
			),
		)
	}
}
