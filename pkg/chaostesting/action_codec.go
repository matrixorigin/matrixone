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
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/reusee/e4"
)

var registeredActionTypes sync.Map

func RegisterAction(value any) {
	t := reflect.TypeOf(value)
	name := t.Name()
	if name == "" {
		panic(fmt.Errorf("Action must be named type: %T", value))
	}
	registeredActionTypes.Store(name, t)
}

var ErrActionNotRegistered = errors.New("action not registered")

func unmarshalAction(d *xml.Decoder, start *xml.StartElement, target *Action) (err error) {

	if start == nil {
		token, err := nextRelevantToken(d)
		if err != nil {
			return we(err)
		}
		s, ok := token.(xml.StartElement)
		if !ok {
			return we(fmt.Errorf("execpting end element"))
		}
		start = &s
	}

	v, ok := registeredActionTypes.Load(start.Name.Local)
	if !ok {
		return we.With(
			e4.Info("action: %s", start.Name.Local),
		)(ErrActionNotRegistered)
	}
	t := v.(reflect.Type)

	ptr := reflect.New(t)
	ce(d.DecodeElement(ptr.Interface(), start))
	*target = ptr.Elem().Interface().(Action)

	return
}
