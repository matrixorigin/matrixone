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

package main

import (
	"fmt"
	"os"
)

func main() {
	argCnt := len(os.Args)
	if argCnt < 2 || argCnt > 3 {
		fmt.Printf("usage: %s definitionFile [outputDiretory]\n", os.Args[0])
		return
	}

	var gen ConfigurationFileGenerator
	if argCnt == 2 {
		gen = NewConfigurationFileGenerator(os.Args[1])
	} else if argCnt == 3 {
		gen = NewConfigurationFileGeneratorWithOutputDirectory(os.Args[1], os.Args[2])
	}

	if err := gen.Generate(); err != nil {
		fmt.Printf("generate system variables failed. error:%v \n", err)
		os.Exit(-1)
	}
}
