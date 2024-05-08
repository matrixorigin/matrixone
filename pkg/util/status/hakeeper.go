// Copyright 2021 -2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package status

import (
	"context"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

type NodeStatus struct {
	NodeID    string                        `json:"node_id"`
	NodeType  string                        `json:"node_type"`
	Address   string                        `json:"address"`
	Labels    map[string]metadata.LabelList `json:"Labels"`
	WorkState metadata.WorkState            `json:"WorkState"`
	Resource  pb.Resource                   `json:"Resource"`
	UpTime    time.Time                     `json:"up_time"`
	DownTime  time.Time                     `json:"down_time"`
}

// HAKeeperStatus contains the status of HAKeeper. Currently, we
// focus on the uptime/downtime of nodes in HAKeeper.
type HAKeeperStatus struct {
	Nodes        []NodeStatus `json:"nodes"`
	DeletedNodes []NodeStatus `json:"deleted_nodes"`
	ErrMsg       string       `json:"err_msg"`
}

func (s *HAKeeperStatus) fill(client logservice.ClusterHAKeeperClient) {
	if client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	details, err := client.GetClusterDetails(ctx)
	if err != nil {
		s.ErrMsg = err.Error()
		return
	}
	for _, cn := range details.CNStores {
		var addr string
		addrItems := strings.Split(cn.SQLAddress, ":")
		if len(addrItems) > 1 {
			addr = addrItems[0]
		}
		s.Nodes = append(s.Nodes, NodeStatus{
			NodeID:    cn.UUID,
			NodeType:  "CN",
			Address:   addr,
			Labels:    cn.Labels,
			WorkState: cn.WorkState,
			Resource:  cn.Resource,
			UpTime:    time.Unix(cn.UpTime/1e9, cn.UpTime%1e9),
		})
	}
	for _, deleted := range details.DeletedStores {
		s.DeletedNodes = append(s.DeletedNodes, NodeStatus{
			NodeID:   deleted.UUID,
			NodeType: deleted.StoreType,
			Address:  deleted.Address,
			UpTime:   time.Unix(deleted.UpTime/1e9, deleted.UpTime%1e9),
			DownTime: time.Unix(deleted.DownTime/1e9, deleted.DownTime%1e9),
		})
	}
}
