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
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestGetTenantInfo(t *testing.T) {
	convey.Convey("tenant", t, func() {
		type input struct {
			input   string
			output  string
			wantErr bool
		}
		args := []input{
			{"u1", "sys:u1:public", false},
			{"tenant1:u1", "tenant1:u1:public", false},
			{"tenant1:u1:r1", "tenant1:u1:r1", false},
			{":u1:r1", "tenant1:u1:r1", true},
			{"tenant1:u1:", "tenant1:u1:r1", true},
			{"tenant1::r1", "tenant1::r1", true},
			{"tenant1:    :r1", "tenant1::r1", true},
			{"     : :r1", "tenant1::r1", true},
			{"   tenant1   :   u1   :   r1    ", "tenant1:u1:r1", false},
		}

		for _, arg := range args {
			ti, err := GetTenantInfo(arg.input)
			if arg.wantErr {
				convey.So(err, convey.ShouldNotBeNil)
			} else {
				convey.So(err, convey.ShouldBeNil)
				tis := ti.String()
				convey.So(tis, convey.ShouldEqual, arg.output)
			}
		}
	})
}
