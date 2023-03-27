// Copyright 2023 Matrix Origin
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

package scan

import (
	"bytes"
	"time"
)

func String(x any, buf *bytes.Buffer) {
	buf.WriteString("simple scan")
}

func Prepare(*proc, any) error {
	return nil
}

func Call(idx int, proc *proc, x any, _, _ bool) (bool, error) {
	defer analyze(proc.GetAnalyze(idx))()

	arg := x.(*Argument)
	bat, err := arg.Reader.Read(proc.Ctx, arg.ColName, nil, proc.GetMPool())
	if err != nil {
		return false, err
	}

	proc.SetInputBatch(bat)
	return true, nil
}

func analyze(a anal) func() {
	t := time.Now()
	a.Start()
	return func() {
		a.Stop()
		a.AddInsertTime(t)
	}
}
