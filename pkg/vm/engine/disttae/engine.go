// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type Engine struct {
	getClusterDetails GetClusterDetailsFunc
}

type GetClusterDetailsFunc = func() (logservice.ClusterDetails, error)

func New(
	ctx context.Context,
	getClusterDetails GetClusterDetailsFunc,
) *Engine {
	return &Engine{
		getClusterDetails: getClusterDetails,
	}
}

var _ engine.Engine = new(Engine)

func (e *Engine) Create(context.Context, string, client.TxnOperator) error {
	//TODO
	panic("unimplemented")
}

func (e *Engine) Database(context.Context, string, client.TxnOperator) (engine.Database, error) {
	//TODO
	panic("unimplemented")
}

func (e *Engine) Databases(context.Context, client.TxnOperator) ([]string, error) {
	//TODO
	panic("unimplemented")
}

func (e *Engine) Delete(context.Context, string, client.TxnOperator) error {
	//TODO
	panic("unimplemented")
}

func (e *Engine) Nodes() (engine.Nodes, error) {
	clusterDetails, err := e.getClusterDetails()
	if err != nil {
		return nil, err
	}

	var nodes engine.Nodes
	for _, store := range clusterDetails.CNStores {
		nodes = append(nodes, engine.Node{
			Mcpu: 1,
			Id:   store.UUID,
			Addr: store.ServiceAddress,
		})
	}

	return nodes, nil
}
