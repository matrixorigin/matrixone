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

package fz

import (
	"bytes"
	"encoding/xml"
	"strings"
	"testing"

	"github.com/reusee/e4"
	"github.com/reusee/sb"
)

func TestActionCodec(t *testing.T) {
	defer he(nil, e4.TestingFatal(t))

	type Case struct {
		Value Action
		XML   string
	}

	cases := []Case{
		{
			Value: Seq(),
			XML:   "<SequentialAction></SequentialAction>",
		},
		{
			Value: Seq(Seq()),
			XML:   "<SequentialAction><SequentialAction></SequentialAction></SequentialAction>",
		},
		{
			Value: Seq(Seq(Seq())),
			XML:   "<SequentialAction><SequentialAction><SequentialAction></SequentialAction></SequentialAction></SequentialAction>",
		},
		{
			Value: Seq(Seq(Seq()), Par()),
			XML:   "<SequentialAction><SequentialAction><SequentialAction></SequentialAction></SequentialAction><ParallelAction></ParallelAction></SequentialAction>",
		},
	}

	for i, c := range cases {
		// encode
		buf := new(bytes.Buffer)
		ce(xml.NewEncoder(buf).Encode(c.Value))
		if !bytes.Equal(buf.Bytes(), []byte(c.XML)) {
			t.Fatalf("bad case: %d", i)
		}
		// decode
		var action Action
		ce(unmarshalAction(
			xml.NewDecoder(strings.NewReader(c.XML)),
			nil,
			&action,
		))
		if sb.MustCompare(
			sb.Marshal(action),
			sb.Marshal(c.Value),
		) != 0 {
			t.Fatalf("bad case: %d", i)
		}
	}

}
