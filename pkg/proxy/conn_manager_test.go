// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/require"
)

func TestTunnelSet(t *testing.T) {
	ts := make(tunnelSet)
	tu := &tunnel{}

	ts.add(tu)
	require.Equal(t, 1, ts.count())
	ts.add(tu)
	require.Equal(t, 1, ts.count())

	ts.add(&tunnel{})
	require.Equal(t, 2, ts.count())

	require.True(t, ts.exists(tu))
	t1 := &tunnel{}
	require.False(t, ts.exists(t1))

	ts.del(tu)
	require.Equal(t, 1, ts.count())
	require.False(t, ts.exists(tu))
}

func TestCNTunnels(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ct := newCNTunnels()
	require.NotNil(t, ct)

	t1 := &tunnel{}
	ct.add("cn1", t1)
	require.Equal(t, 1, ct.count())

	t2 := &tunnel{}
	ct.add("cn1", t2)
	require.Equal(t, 2, ct.count())

	// same tunnel
	ct.add("cn1", t2)
	require.Equal(t, 2, ct.count())

	ct.add("cn1", nil)
	require.Equal(t, 2, ct.count())

	t3 := &tunnel{}
	ct.add("cn2", t3)
	require.Equal(t, 3, ct.count())

	// no this cn.
	ct.del("no-this-cn", t1)
	require.Equal(t, 3, ct.count())

	// tunnel is not on this cn.
	ct.del("cn2", t1)
	require.Equal(t, 3, ct.count())

	ct.del("cn1", t1)
	require.Equal(t, 2, ct.count())

	ct.del("cn1", t1)
	require.Equal(t, 2, ct.count())
	ct.del("cn1", t2)
	require.Equal(t, 1, ct.count())
	ct.del("cn2", t3)
	require.Equal(t, 0, ct.count())

	ct.del("cn2", t3)
	require.Equal(t, 0, ct.count())
}

func TestConnManagerConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cm := newConnManager()
	require.NotNil(t, cm)

	cn11 := &CNServer{
		hash: "hash1",
		reqLabel: newLabelInfo("t1", map[string]string{
			"k1": "v1",
		}),
		uuid: "cn11",
	}
	cn12 := &CNServer{
		hash: "hash1",
		reqLabel: newLabelInfo("t1", map[string]string{
			"k1": "v1",
		}),
		uuid: "cn12",
	}
	cn21 := &CNServer{
		hash: "hash2",
		reqLabel: newLabelInfo("t1", map[string]string{
			"k2": "v2",
		}),
		uuid: "cn21",
	}

	tu0 := newTunnel(context.TODO(), nil)

	tu11 := newTunnel(context.TODO(), nil)
	cm.connect(cn11, tu11)
	require.Equal(t, 1, cm.count())
	require.Equal(t, 1, len(cm.getLabelHashes()))
	require.Equal(t, 1, cm.getCNTunnels("hash1").count())
	require.Equal(t, 0, cm.getCNTunnels("hash2").count())

	tu12 := newTunnel(context.TODO(), nil)
	cm.connect(cn12, tu12)
	require.Equal(t, 2, cm.count())
	require.Equal(t, 1, len(cm.getLabelHashes()))
	require.Equal(t, 2, cm.getCNTunnels("hash1").count())
	require.Equal(t, 0, cm.getCNTunnels("hash2").count())

	tu21 := newTunnel(context.TODO(), nil)
	cm.connect(cn21, tu21)
	require.Equal(t, 3, cm.count())
	require.Equal(t, 2, len(cm.getLabelHashes()))
	require.Equal(t, 2, cm.getCNTunnels("hash1").count())
	require.Equal(t, 1, cm.getCNTunnels("hash2").count())

	cm.disconnect(cn12, tu11)
	require.Equal(t, 3, cm.count())
	require.Equal(t, 2, len(cm.getLabelHashes()))
	require.Equal(t, 2, cm.getCNTunnels("hash1").count())
	require.Equal(t, 1, cm.getCNTunnels("hash2").count())

	cm.disconnect(cn11, tu0)
	require.Equal(t, 3, cm.count())
	require.Equal(t, 2, len(cm.getLabelHashes()))
	require.Equal(t, 2, cm.getCNTunnels("hash1").count())
	require.Equal(t, 1, cm.getCNTunnels("hash2").count())

	cm.disconnect(cn12, tu12)
	require.Equal(t, 2, cm.count())
	require.Equal(t, 2, len(cm.getLabelHashes()))
	require.Equal(t, 1, cm.getCNTunnels("hash1").count())
	require.Equal(t, 1, cm.getCNTunnels("hash2").count())

	cm.disconnect(cn12, tu0)
	require.Equal(t, 2, cm.count())
	require.Equal(t, 2, len(cm.getLabelHashes()))
	require.Equal(t, 1, cm.getCNTunnels("hash1").count())
	require.Equal(t, 1, cm.getCNTunnels("hash2").count())

	cm.disconnect(cn11, tu11)
	require.Equal(t, 1, cm.count())
	require.Equal(t, 1, len(cm.getLabelHashes()))
	require.Equal(t, 0, cm.getCNTunnels("hash1").count())
	require.Equal(t, 1, cm.getCNTunnels("hash2").count())

	cm.disconnect(cn21, tu21)
	require.Equal(t, 0, cm.count())
	require.Equal(t, 0, len(cm.getLabelHashes()))
	require.Equal(t, 0, cm.getCNTunnels("hash1").count())
	require.Equal(t, 0, cm.getCNTunnels("hash2").count())

	cm.disconnect(cn21, tu0)
	require.Equal(t, 0, cm.count())
	require.Equal(t, 0, len(cm.getLabelHashes()))
	require.Equal(t, 0, cm.getCNTunnels("hash1").count())
	require.Equal(t, 0, cm.getCNTunnels("hash2").count())
}

func TestConnManagerConnectionConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cm := newConnManager()
	require.NotNil(t, cm)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(j int) {
			cn11 := &CNServer{
				hash: "hash1",
				reqLabel: newLabelInfo("t1", map[string]string{
					"k1": "v1",
				}),
				uuid: fmt.Sprintf("cn1-%d", j),
			}
			tu11 := newTunnel(context.TODO(), nil)
			cm.connect(cn11, tu11)
			wg.Done()
		}(i)
		go func(j int) {
			cn11 := &CNServer{
				hash: "hash2",
				reqLabel: newLabelInfo("t1", map[string]string{
					"k2": "v2",
				}),
				uuid: fmt.Sprintf("cn2-%d", j),
			}
			tu11 := newTunnel(context.TODO(), nil)
			cm.connect(cn11, tu11)
			wg.Done()
		}(i)
	}
	wg.Wait()

	require.Equal(t, 200, cm.count())
	require.Equal(t, 2, len(cm.getLabelHashes()))
	require.Equal(t, 100, cm.getCNTunnels("hash1").count())
	require.Equal(t, 100, cm.getCNTunnels("hash2").count())
}

func TestConnManagerLabelInfo(t *testing.T) {
	cm := newConnManager()
	require.NotNil(t, cm)

	cn11 := &CNServer{
		hash: "hash1",
		reqLabel: newLabelInfo("t1", map[string]string{
			"k1": "v1",
		}),
		uuid: "cn11",
	}

	tu11 := newTunnel(context.TODO(), nil)
	cm.connect(cn11, tu11)
	require.Equal(t, 1, cm.count())
	require.Equal(t, 1, len(cm.getLabelHashes()))
	require.Equal(t, 1, cm.getCNTunnels("hash1").count())
	require.Equal(t, 0, cm.getCNTunnels("hash2").count())

	li := cm.getLabelInfo("hash1")
	require.Equal(t, labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
		},
	}, li)
}
