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
	"runtime"
)

// jiebaDictPaths returns the five dict paths gojieba.NewJieba expects, in the
// order it documents: dict, hmm, user_dict, idf, stop_words.
//
// Resolution order:
//  1. MO_JIEBA_DICT_DIR env var, when set and pointing at a directory that
//     contains jieba.dict.utf8.
//  2. The vendored ./dict directory next to this source file (works for
//     `go test` and any build where the source tree is still on disk).
//
// Upstream gojieba defaults to a path inside its own module cache directory,
// which is fragile: the binary embeds an absolute path determined at compile
// time and panics at runtime if that path is missing (e.g. multi-stage Docker
// builds, CI runners with a stripped GOMODCACHE). Passing explicit paths
// sidesteps that.
func jiebaDictPaths() [5]string {
	dir := resolveJiebaDictDir()
	return [5]string{
		filepath.Join(dir, "jieba.dict.utf8"),
		filepath.Join(dir, "hmm_model.utf8"),
		filepath.Join(dir, "user.dict.utf8"),
		filepath.Join(dir, "idf.utf8"),
		filepath.Join(dir, "stop_words.utf8"),
	}
}

func resolveJiebaDictDir() string {
	if d := os.Getenv("MO_JIEBA_DICT_DIR"); d != "" {
		if _, err := os.Stat(filepath.Join(d, "jieba.dict.utf8")); err == nil {
			return d
		}
	}
	_, self, _, ok := runtime.Caller(0)
	if ok {
		d := filepath.Join(filepath.Dir(self), "dict")
		if _, err := os.Stat(filepath.Join(d, "jieba.dict.utf8")); err == nil {
			return d
		}
	}
	return ""
}
