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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
	"github.com/reusee/e4"
)

func TestPorcupine(t *testing.T) {
	defer he(nil, e4.TestingFatal(t))

	// porcupine operations
	var operations []porcupine.Operation
	var opsLock sync.Mutex

	// shared kv
	kv := NewKV(128)

	// nodes
	var nodes []*TestPorcupineNode

	fz.NewScope(

		&fz.Operators{

			// porcupine checker
			fz.PorcupineChecker(
				fz.PorcupineKVModel,
				func() []porcupine.Operation {
					return operations
				},
				nil,
				time.Second*5,
			),

			// show ops
			//fz.Operator{
			//	AfterStop: func() {
			//		for _, op := range operations {
			//			pt("%+v\n", op)
			//		}
			//	},
			//},
		},
	).Fork(

		// main action
		func(
			numNodes fz.NumNodes,
		) fz.MainAction {
			num := int64(0)
			return fz.MainAction{
				// random tree
				Action: fz.RandomActionTree(
					[]fz.ActionMaker{
						// set / get pair
						func() fz.Action {
							key := atomic.AddInt64(&num, 1)
							value := atomic.AddInt64(&num, 1)
							node := fz.NodeID(rand.Intn(int(numNodes)))
							return fz.Seq(
								ActionSetAtNode{
									Key:   key,
									Value: value,
									Node:  node,
								},
								ActionGetAtNode{
									Key:  key,
									Node: node,
								},
							)
						},
					},
					2048,
				),
			}
		},

		// start node
		func() fz.StartNode {
			return func(id fz.NodeID) (fz.Node, error) {
				node := &TestPorcupineNode{
					ID: id,
					KV: kv,
				}
				nodes = append(nodes, node)
				return node, nil
			}
		},

		// do
		func() fz.Do {
			return func(action fz.Action) error {

				switch action := action.(type) {

				case ActionGetAtNode:
					// get
					node := nodes[action.Node]
					t0 := time.Now()
					value := node.KV.Get(action.Key)
					t1 := time.Now()
					// save operation
					opsLock.Lock()
					operations = append(operations, porcupine.Operation{
						ClientId: int(action.Node),
						Input:    [2]any{"get", action.Key},
						Output:   value,
						Call:     t0.UnixNano(),
						Return:   t1.UnixNano(),
					})
					opsLock.Unlock()

				case ActionSetAtNode:
					// set
					node := nodes[action.Node]
					t0 := time.Now()
					node.KV.Set(action.Key, action.Value)
					t1 := time.Now()
					// save operation
					opsLock.Lock()
					operations = append(operations, porcupine.Operation{
						ClientId: int(action.Node),
						Input:    [2]any{"set", action.Key},
						Output:   action.Value,
						Call:     t0.UnixNano(),
						Return:   t1.UnixNano(),
					})
					opsLock.Unlock()

				default:
					panic(fmt.Errorf("unknown action: %T", action))

				}

				return nil
			}
		},
	).Call(func(
		execute fz.Execute,
	) {
		// execute
		ce(execute())
	})

}

type TestPorcupineNode struct {
	ID fz.NodeID
	KV *KV
}

func (t *TestPorcupineNode) Close() error {
	return nil
}
