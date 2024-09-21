// Copyright 2021-2024 Matrix Origin
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

package shardservice

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

var (
	sid = ""
)

func TestHasCN(t *testing.T) {
	initTestCluster("cn1")
	e := NewEnv(sid, "")
	require.True(t, e.HasCN("cn1"))
	require.False(t, e.HasCN("cn2"))
}

func TestAvailable(t *testing.T) {
	initTestCluster("cn1:tenant:1,cn2:tenant:2,cn3")
	e := NewEnv(sid, "tenant")
	require.True(t, e.Available(1, "cn1"))
	require.False(t, e.Available(2, "cn1"))
	require.False(t, e.Available(1, "cn2"))
}

// cn:label_name:label_value,cn2:label_name:label_value,cn3
func initTestCluster(
	services string,
) ([]metadata.CNService, metadata.TNService) {
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
	testSockets := fmt.Sprintf("unix:///tmp/%d.sock", time.Now().Nanosecond())
	cnInfo := strings.Split(services, ",")
	cns := make([]metadata.CNService, 0, len(cnInfo))
	tn := metadata.TNService{
		ServiceID:           "tn",
		ShardServiceAddress: testSockets,
	}
	runtime.SetupServiceBasedRuntime(tn.ServiceID, runtime.ServiceRuntime(sid))
	for _, info := range cnInfo {
		cn := metadata.CNService{}
		if !strings.Contains(info, ":") {
			cn.ServiceID = info
		} else {
			values := strings.Split(info, ":")
			cn.ServiceID = values[0]
			cn.Labels = make(map[string]metadata.LabelList)
			cn.Labels[values[1]] = metadata.LabelList{Labels: []string{values[2]}}
		}
		cn.ShardServiceAddress = fmt.Sprintf("unix:///tmp/service-%d-%s.sock",
			time.Now().Nanosecond(), cn.ServiceID)

		cns = append(cns, cn)

		runtime.SetupServiceBasedRuntime(cn.ServiceID, runtime.ServiceRuntime(sid))
	}

	runtime.ServiceRuntime(sid).SetGlobalVariables(
		runtime.ClusterService,
		clusterservice.NewMOCluster(
			sid,
			nil,
			0,
			clusterservice.WithDisableRefresh(),
			clusterservice.WithServices(
				cns,
				[]metadata.TNService{tn},
			),
		))
	return cns, tn
}
