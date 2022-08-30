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

package external

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/smartystreets/goconvey/convey"
)

const (
	Rows = 10 // default rows
)

// add unit tests for cases
type externalTestCase struct {
	arg    *Argument
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

var (
	tcs externalTestCase
)

func newTestCase(gm *guest.Mmu, all bool) externalTestCase {
	proc := testutil.NewProcess()
	proc.FileService = testutil.NewFS()
	ctx, cancel := context.WithCancel(context.Background())
	return externalTestCase{
		proc: proc,
		types: []types.Type{
			{Oid: types.T_int8},
		},
		arg: &Argument{
			Es: &ExternalParam{
				Ctx: ctx,
			},
		},
		cancel: cancel,
	}
}

func init() {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	tcs = newTestCase(gm, true)
}

func Test_String(t *testing.T) {
	buf := new(bytes.Buffer)
	String(tcs.arg, buf)
}

func Test_Prepare(t *testing.T) {
	convey.Convey("external Prepare", t, func() {
		param := tcs.arg.Es
		err := Prepare(tcs.proc, tcs.arg)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(param.extern, convey.ShouldNotBeNil)
		convey.So(param.End, convey.ShouldBeTrue)

		extern := &tree.ExternParam{
			Filepath: "",
			Tail: &tree.TailParameter{
				IgnoredLines: 0,
			},
			FileService: tcs.proc.FileService,
		}
		json_byte, err := json.Marshal(extern)
		if err != nil {
			panic(err)
		}
		param.CreateSql = string(json_byte)
		err = Prepare(tcs.proc, tcs.arg)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(param.FileList, convey.ShouldBeNil)
		convey.So(param.FileCnt, convey.ShouldEqual, 0)

		json_byte, err = json.Marshal(extern)
		if err != nil {
			panic(err)
		}
		param.CreateSql = string(json_byte)
		err = Prepare(tcs.proc, tcs.arg)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(param.FileList, convey.ShouldBeNil)
		convey.So(param.FileCnt, convey.ShouldEqual, 0)
	})
}

func Test_Call(t *testing.T) {
	convey.Convey("external Call", t, func() {
		param := tcs.arg.Es
		extern := &tree.ExternParam{
			Filepath: "",
			Tail: &tree.TailParameter{
				IgnoredLines: 0,
			},
			FileService: tcs.proc.FileService,
		}
		param.extern = extern
		param.End = false
		param.FileList = []string{"abc.txt"}
		end, err := Call(1, tcs.proc, tcs.arg)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(end, convey.ShouldBeFalse)

		param.End = false
		end, err = Call(1, tcs.proc, tcs.arg)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(end, convey.ShouldBeFalse)
	})
}
