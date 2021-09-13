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
	"matrixone/pkg/logutil"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/vm/engine/aoe/common/codec"
	"os"
	"testing"
)

func Test_readTextFile(t *testing.T) {
	data,err := os.ReadFile("test/loadfile.csv")
	require.NoError(t, err)
	fmt.Printf("%v\n",data)
}

func loadDataFromFile(t *testing.T,f string)[]byte{
	data,err := os.ReadFile(f)
	require.NoError(t, err)
	return data
}

func loadAndProcess(t *testing.T,load *tree.Load,packline func([][][]byte)) {
	/*
		step1 : read block from file
	*/
	dataFile,err := os.Open(load.File)
	if err != nil {
		logutil.Errorf("open file failed. err:%v",err)
		return
	}
	defer func() {
		err := dataFile.Close()
		if err != nil{
			logutil.Errorf("close file failed. err:%v",err)
		}
	}()
}

func Test_loadAndProcess(t *testing.T) {
	/*
	//ssb
	date.tbl
	customer.tbl
	part.tbl
	supplier.tbl
	lineorder.tbl
	 */
	f1 := "../../../mo-test/part.tbl"
	t1 := &tree.Load{
		Local:             false,
		File:              f1,
		DuplicateHandling: nil,
		Table:             nil,
		Fields:            &tree.Fields{
			Terminated: "|",
			Optionally: true,
			EnclosedBy: '"',
			EscapedBy:  '\\',
		},
		Lines:             &tree.Lines{
			StartingBy:   "",
			TerminatedBy: "\n",//careful. \\n -> "\n" , \n -> '\n'
		},
		IgnoredLines:      0,
		ColumnList:        nil,
		Assignments:       nil,
	}

	t1_packline := func(lineArray [][][]byte) {
		for _, line := range lineArray {
			var oline []byte = nil
			for _,field := range line{
				oline = append(oline,field...)
				oline = append(oline,byte('|'))
			}
			fmt.Printf("%s\n",codec.Bytes2String(oline))
		}
	}

	loadAndProcess(t,t1,t1_packline)
}

func Test_loadAndProcess2(t *testing.T) {
	/*
		//taxi
	central_park_weather.csv -- further check
	fhv_bases.csv
	taxi_head_10-2
	*/
	f1 := "../../../mo-test/taxi_head_10-2"
	t1 := &tree.Load{
		Local:             false,
		File:              f1,
		DuplicateHandling: nil,
		Table:             nil,
		Fields:            &tree.Fields{
			Terminated: ",",
			Optionally: true,
			EnclosedBy: '"',
			EscapedBy:  '\\',
		},
		Lines:             &tree.Lines{
			StartingBy:   "",
			TerminatedBy: "\n",//careful. \\n -> "\n" , \n -> '\n'
		},
		IgnoredLines:      0,
		ColumnList:        nil,
		Assignments:       nil,
	}

	lastFieldAppend := true
	t1_packline := func(lineArray [][][]byte) {
		for _, line := range lineArray {
			var oline []byte = nil
			for i,field := range line{
				oline = append(oline,field...)
				if lastFieldAppend == true {
					oline = append(oline,byte(','))
				}else{
					if i < len(line) - 1 {
						oline = append(oline,byte(','))
					}
				}

			}
			fmt.Printf("%s\n",codec.Bytes2String(oline))
		}
	}

	loadAndProcess(t,t1,t1_packline)
}