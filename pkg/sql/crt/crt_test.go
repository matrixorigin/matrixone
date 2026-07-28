// Copyright 2022 - 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crt

import (
	"context"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/smartystreets/goconvey/convey"
)

func Test_getCompressType(t *testing.T) {
	convey.Convey("getCompressType succ", t, func() {
		param := &tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				CompressType: tree.GZIP,
			},
			ExParam: tree.ExParam{
				Ctx: context.Background(),
			},
		}
		compress := GetCompressType(param.CompressType, param.Filepath)
		convey.So(compress, convey.ShouldEqual, param.CompressType)

		param.CompressType = tree.AUTO
		param.Filepath = "a.gz"
		compress = GetCompressType(param.CompressType, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.GZIP)

		param.Filepath = "a.bz2"
		compress = GetCompressType(param.CompressType, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.BZIP2)

		param.Filepath = "a.lz4"
		compress = GetCompressType(param.CompressType, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.LZ4)

		param.Filepath = "a.csv"
		compress = GetCompressType(param.CompressType, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.NOCOMPRESS)

		param.Filepath = "a"
		compress = GetCompressType(param.CompressType, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.NOCOMPRESS)
	})
}

func Test_getUnCompressReader(t *testing.T) {
	convey.Convey("getUnCompressReader succ", t, func() {
		param := &tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				CompressType: tree.NOCOMPRESS,
			},
		}

		ctx := context.Background()

		read, err := getUnCompressReader(ctx, param.CompressType, param.Filepath, nil)
		convey.So(read, convey.ShouldBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.BZIP2
		read, err = getUnCompressReader(ctx, param.CompressType, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.FLATE
		read, err = getUnCompressReader(ctx, param.CompressType, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.LZ4
		read, err = getUnCompressReader(param.Ctx, param.CompressType, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.LZW
		read, err = getUnCompressReader(ctx, param.CompressType, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		param.CompressType = "abc"
		read, err = getUnCompressReader(ctx, param.CompressType, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}
