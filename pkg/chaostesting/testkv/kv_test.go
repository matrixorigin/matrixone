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

package testkv

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"

	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
	"github.com/reusee/e4"
)

func TestKV(t *testing.T) {
	defer he(nil, e4.TestingFatal(t))

	scope := fz.NewScope(

		// config items
		func(
			maxClients MaxClients,
		) fz.ConfigItems {
			return fz.ConfigItems{maxClients}
		},

		// max clients
		func() MaxClients {
			return 8
		},
	).Fork(

		// main action
		&fz.MainAction{
			Action: fz.RandomActionTree([]fz.ActionMaker{
				func() fz.Action {
					key := rand.Int63()
					value := rand.Int63()
					return fz.Seq(
						ActionSet{
							Key:   key,
							Value: value,
						},
						ActionGet{
							Key: key,
						},
					)
				},
			}, 128),
		},

		// cpu profile
		func() fz.EnableCPUProfile {
			return true
		},
	)

	// config write
	var writeConfig fz.WriteConfig
	scope.Assign(&writeConfig)
	f, err := os.Create("config.xml")
	ce(err)
	ce(writeConfig(f))
	ce(f.Close())

	// config read
	var readConfig fz.ReadConfig
	scope.Assign(&readConfig)
	content, err := os.ReadFile("config.xml")
	ce(err)
	defs, err := readConfig(bytes.NewReader(content))
	ce(err)
	scope = scope.Fork(defs...)

	var kv *KV

	scope = scope.Fork(
		func(
			maxClients MaxClients,
		) (
			start fz.StartNode,
			do fz.Do,
		) {

			// Start
			start = func(id fz.NodeID) (fz.Node, error) {
				kv = NewKV(int(maxClients))
				return &TestKVNode{}, nil
			}

			// Do
			do = func(threadID int64, action fz.Action) error {
				switch action := action.(type) {

				case ActionSet:
					kv.Set(action.Key, action.Value)

				case ActionGet:
					kv.Get(action.Key)

				default:
					panic(fmt.Errorf("unknown action: %T", action))
				}
				return nil
			}

			return
		},
		&fz.Operators{
			fz.Operator{
				AfterStop: func() {
				},
			},
		},
	)

	scope.Call(func(
		execute fz.Execute,
	) {
		ce(execute())
	})

}

type TestKVNode struct{}

func (t *TestKVNode) Close() error {
	return nil
}

type MaxClients int
