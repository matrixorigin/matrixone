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
	"bytes"
	"io"
	"os"
	"path"
	"runtime"
	"testing"
)

func readFile(t *testing.T, filename string) []byte {
	_, fn, _, _ := runtime.Caller(0)
	dir := path.Dir(fn)
	fpath := path.Join(dir, "..", "data", filename)
	file, err := os.Open(fpath)
	if err != nil {
		t.Errorf("Failed to open file %s: %v", filename, err)
		return nil
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		t.Errorf("Failed to read file %s: %v", filename, err)
		return nil
	}
	return data
}

func TestShakespear(tt *testing.T) {
	data := readFile(tt, "t8.shakespeare.txt")
	if data == nil {
		tt.Errorf("Failed to read shakespeare.txt")
		return
	}

	var cntRomeo, cntJoliet, bMax, tMax int32

	tknz, _ := NewSimpleTokenizer(data)
	for t := range tknz.Tokenize() {
		if t.TokenBytes[0] == 5 && string(t.TokenBytes[1:6]) == "romeo" {
			cntRomeo++
		} else if t.TokenBytes[0] == 6 && string(t.TokenBytes[1:7]) == "juliet" {
			cntJoliet++
		}

		bMax = t.BytePos
		tMax = t.TokenPos
	}

	tt.Log("Romeo Count:", cntRomeo, "Julient Count:", cntJoliet)
	tt.Log("LastToken:", tMax, "At Byte:", bMax)
}

func TestHLM(tt *testing.T) {
	gbkdata := readFile(tt, "红楼梦.txt")
	if gbkdata == nil {
		tt.Errorf("Failed to read hlm.txt")
		return
	}

	reader := bytes.NewReader(gbkdata)
	//tr := transform.NewReader(reader, simplifiedchinese.GBK.NewDecoder())
	data, err := io.ReadAll(reader)
	if err != nil {
		tt.Errorf("Failed to decode hlm.txt: %v", err)
		return
	}

	var cntJBY, cntLDY, bMax, tMax int32

	tknz, _ := NewSimpleTokenizer(data)
	for t := range tknz.Tokenize() {
		if t.TokenBytes[0] == 9 && string(t.TokenBytes[1:10]) == "贾宝玉" {
			origString := string(data[t.BytePos : t.BytePos+9])
			if origString != "贾宝玉" {
				tt.Error("Mismatched JBY")
			}
			cntJBY++
		} else if t.TokenBytes[0] == 9 && string(t.TokenBytes[1:10]) == "林黛玉" {
			origString := string(data[t.BytePos : t.BytePos+9])
			if origString != "林黛玉" {
				tt.Error("Mismatched LDY")
			}
			cntLDY++
		}

		tMax = t.TokenPos
		bMax = t.BytePos
	}
	tt.Log("JBY Count:", cntJBY, "LDY Count:", cntLDY)
	tt.Log("LastToken:", tMax, "At Byte:", bMax)
}
