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

package main

import (
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/chaostesting"
)

func (_ Def2) Do(
	kvs KVs,
) fz.Do {

	return func(action fz.Action) error {

		//TODO make porcupine operations
		pt("%+v\n", action)

		switch action := action.(type) {

		case ActionSet:
			kv := kvs[action.NodeID]
			return kv.Set(action.Key, action.Value, time.Second*5)

		case ActionGet:
			kv := kvs[action.NodeID]
			var res int
			return kv.Get(action.Key, &res, time.Second*5)

		default:
			panic(fmt.Errorf("unknown action: %#v", action))

		}

	}
}
