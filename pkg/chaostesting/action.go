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

type Action interface {
}

type ActionMaker = func() Action

type MainAction struct {
	Action Action
}

func (_ Def) MainAction() (m MainAction) {
	panic(fmt.Errorf("fixme: provide %T", m))
}

var _ xml.Unmarshaler = new(MainAction)

func (t *MainAction) UnmarshalXML(d *xml.Decoder, start xml.StartElement) (err error) {
	token, err := nextRelevantToken(d)
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
	start = token.(xml.StartElement)
	if err := unmarshalAction(d, &start, &t.Action); err != nil {
		return we(err)
	}
	if err := d.Skip(); err != nil {
		return we(err)
	}
	return nil
}

func (_ Def) MainActionConfigItem(
	action MainAction,
) ConfigItems {
	return ConfigItems{action}
}
