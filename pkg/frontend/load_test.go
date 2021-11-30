// Copyright 2021 Matrix Origin
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

package frontend

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"os"
	"testing"
)

func Test_readTextFile(t *testing.T) {
	data, err := os.ReadFile("test/loadfile.csv")
	require.NoError(t, err)
	fmt.Printf("%v\n", data)
}

func loadDataFromFile(t *testing.T, f string) []byte {
	data, err := os.ReadFile(f)
	require.NoError(t, err)
	return data
}

func loadAndProcess(t *testing.T, load *tree.Load, packline func([][][]byte)) {
	/*
		step1 : read block from file
	*/
	dataFile, err := os.Open(load.File)
	if err != nil {
		logutil.Errorf("open file failed. err:%v", err)
		return
	}
	defer func() {
		err := dataFile.Close()
		if err != nil {
			logutil.Errorf("close file failed. err:%v", err)
		}
	}()
}

func Test_loadAndProcess(t *testing.T) {

}

func Test_loadAndProcess2(t *testing.T) {

}
