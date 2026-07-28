// Copyright 2024 Matrix Origin
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

package tokenizer

import (
	"os"
	"path/filepath"
	"testing"
)

// writeFakeDict creates a minimally-populated dict directory. Only
// jieba.dict.utf8 needs to exist for resolveJiebaDictDir to accept the dir;
// the other files are checked at NewJieba time, not at resolution time.
func writeFakeDict(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "jieba.dict.utf8"), nil, 0o644); err != nil {
		t.Fatalf("write fake dict: %v", err)
	}
}

func TestResolveJiebaDictDirEnvVar(t *testing.T) {
	tmp := t.TempDir()
	writeFakeDict(t, tmp)

	t.Setenv("MO_JIEBA_DICT_DIR", tmp)
	got := resolveJiebaDictDir()
	if got != tmp {
		t.Errorf("resolveJiebaDictDir() = %q, want %q", got, tmp)
	}
}

func TestResolveJiebaDictDirEnvFallthroughWhenMissing(t *testing.T) {
	// Env var set but the directory has no jieba.dict.utf8 — must fall through
	// to a later resolution step rather than returning the bogus path.
	tmp := t.TempDir()
	t.Setenv("MO_JIEBA_DICT_DIR", tmp)
	got := resolveJiebaDictDir()
	if got == tmp {
		t.Errorf("resolveJiebaDictDir() = %q, want fallback (env dir has no jieba.dict.utf8)", got)
	}
}

func TestDictDirOK(t *testing.T) {
	if dictDirOK("") {
		t.Error("dictDirOK(\"\") = true, want false")
	}
	if dictDirOK("/nonexistent/path/that/does/not/exist") {
		t.Error("dictDirOK(nonexistent) = true, want false")
	}
	tmp := t.TempDir()
	if dictDirOK(tmp) {
		t.Error("dictDirOK(empty dir) = true, want false")
	}
	writeFakeDict(t, tmp)
	if !dictDirOK(tmp) {
		t.Error("dictDirOK(dir with jieba.dict.utf8) = false, want true")
	}
}

func TestJiebaDictPathsLayout(t *testing.T) {
	// The five paths returned must be siblings under one directory and named
	// exactly as gojieba.NewJieba expects.
	tmp := t.TempDir()
	writeFakeDict(t, tmp)
	t.Setenv("MO_JIEBA_DICT_DIR", tmp)

	paths := jiebaDictPaths()
	wantBases := []string{
		"jieba.dict.utf8",
		"hmm_model.utf8",
		"user.dict.utf8",
		"idf.utf8",
		"stop_words.utf8",
	}
	for i, p := range paths {
		if filepath.Dir(p) != tmp {
			t.Errorf("paths[%d] dir = %q, want %q", i, filepath.Dir(p), tmp)
		}
		if filepath.Base(p) != wantBases[i] {
			t.Errorf("paths[%d] base = %q, want %q", i, filepath.Base(p), wantBases[i])
		}
	}
}
