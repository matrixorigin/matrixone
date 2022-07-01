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

package txnif

import (
	"fmt"
	"io"
)

type TxnCmd interface {
	WriteTo(io.Writer) (int64, error)
	ReadFrom(io.Reader) (int64, error)
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	GetType() int16
	Desc() string
	String() string
	VerboseString() string
	Close()
}

type CmdFactory func(int16) TxnCmd

var cmdFactories = map[int16]CmdFactory{}

func RegisterCmdFactory(cmdType int16, factory CmdFactory) {
	_, ok := cmdFactories[cmdType]
	if ok {
		panic(fmt.Sprintf("duplicate cmd type: %d", cmdType))
	}
	cmdFactories[cmdType] = factory
}

func GetCmdFactory(cmdType int16) (factory CmdFactory) {
	factory = cmdFactories[cmdType]
	if factory == nil {
		panic(fmt.Sprintf("no factory found for cmd: %d", cmdType))
	}
	return
}
