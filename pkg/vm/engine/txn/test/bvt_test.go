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
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testBVT(t *testing.T, dir string) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	env, err := newEnv(ctx)
	assert.Nil(t, err)
	defer func() {
		if err := env.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	session := env.NewSession()

	err = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}

		content, err := os.ReadFile(path)
		assert.Nil(t, err)
		//fmt.Printf("FILE: %s\n", path)

		// trim comment lines
		lines := bytes.Split(content, []byte("\n"))
		filtered := lines[:0]
		for _, line := range lines {
			if bytes.HasPrefix(line, []byte("--")) {
				continue
			}
			if bytes.HasPrefix(line, []byte("#")) {
				continue
			}
			filtered = append(filtered, line)
		}
		content = bytes.Join(filtered, []byte("\n"))

		err = session.Exec(ctx, path, string(content))
		if err != nil {
			panic(err)
		}

		return nil
	})
	assert.Nil(t, err)

}

func TestTPCH(t *testing.T) {
	testBVT(
		t,
		filepath.Join(
			"..", "..", "..", "..", "..",
			"test", "cases", "benchmark", "tpch",
		),
	)
}

func TestDML(t *testing.T) {
	testBVT(
		t,
		filepath.Join(
			"..", "..", "..", "..", "..",
			"test", "cases", "dml",
		),
	)
}

func TestBVT(t *testing.T) {
	testBVT(
		t,
		filepath.Join(
			"..", "..", "..", "..", "..",
			"test", "cases",
		),
	)
}
