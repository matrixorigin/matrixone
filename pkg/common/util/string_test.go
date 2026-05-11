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

package util

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestSubStringFromBegin(t *testing.T) {
	convey.Convey("SubStringFromBegin", t, func() {
		// Test normal truncation
		convey.So(Abbreviate("abcdef", 3), convey.ShouldEqual, "abc...")

		// Test string shorter than length
		convey.So(Abbreviate("abc", 5), convey.ShouldEqual, "abc")

		// Test exact length match
		convey.So(Abbreviate("abc", 3), convey.ShouldEqual, "abc")

		// Test empty string
		convey.So(Abbreviate("", 5), convey.ShouldEqual, "")

		// Test length 0
		convey.So(Abbreviate("abcdef", 0), convey.ShouldEqual, "")

		// Test length -1 (return complete string)
		convey.So(Abbreviate("abcdef", -1), convey.ShouldEqual, "abcdef")

		// Test length < -1
		convey.So(Abbreviate("abcdef", -2), convey.ShouldEqual, "")

		// Test long string
		longStr := "a" + string(make([]byte, 2000))
		result := Abbreviate(longStr, 1024)
		convey.So(len(result), convey.ShouldEqual, 1027) // 1024 + "..."
		convey.So(result[:1024], convey.ShouldEqual, longStr[:1024])
		convey.So(result[1024:], convey.ShouldEqual, "...")
	})
}
