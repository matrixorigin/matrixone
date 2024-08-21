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

package embed

import (
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

// Cluster is the mo cluster interface
type Cluster interface {
	ID() uint64
	Start() error
	Close() error
	GetService(sid string) (ServiceOperator, error)
	GetCNService(index int) (ServiceOperator, error)
	ForeachServices(fn func(ServiceOperator) bool)
}

type Option func(*cluster)

type ServiceOperator interface {
	ServiceID() string
	ServiceType() metadata.ServiceType
	Index() int
	Adjust(func(*ServiceConfig))
	RawService() interface{}
	GetServiceConfig() ServiceConfig

	Start() error
	Close() error
}
