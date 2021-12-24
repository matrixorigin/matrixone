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
	"encoding/xml"
	"fmt"
)

type ParallelAction struct {
	Actions []Action
}

func init() {
	RegisterAction(ParallelAction{})
}

func Par(actions ...Action) ParallelAction {
	return ParallelAction{
		Actions: actions,
	}
}

var _ Action = ParallelAction{}

var _ xml.Marshaler = ParallelAction{}

func (s ParallelAction) MarshalXML(e *xml.Encoder, start xml.StartElement) (err error) {
	defer he(&err)

	ce(e.EncodeToken(xml.StartElement{
		Name: xml.Name{
			Local: "ParallelAction",
		},
	}))

	for _, action := range s.Actions {
		ce(e.Encode(action))
	}

	ce(e.EncodeToken(xml.EndElement{
		Name: xml.Name{
			Local: "ParallelAction",
		},
	}))

	return
}

var _ xml.Unmarshaler = new(ParallelAction)

func (s *ParallelAction) UnmarshalXML(d *xml.Decoder, start xml.StartElement) (err error) {
	defer he(&err)

	for {
		token, err := nextTokenSkipCharData(d)
		if err != nil {
			return we(err)
		}
		if end, ok := token.(xml.EndElement); ok {
			if end.Name != start.Name {
				return we(xml.UnmarshalError(fmt.Sprintf(
					"expecting end of %s, got %s", start.Name.Local, end.Name.Local)))
			}
			return nil
		}
		var action Action
		start := token.(xml.StartElement)
		ce(unmarshalAction(d, &start, &action))
		if action != nil {
			s.Actions = append(s.Actions, action)
		}
	}

}
