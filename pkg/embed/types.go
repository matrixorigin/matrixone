package embed

import (
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

// Cluster is the mo cluster interface
type Cluster interface {
	Start() error
	Close() error
	GetService(sid string) (ServiceOperator, error)
	ForeachServices(fn func(ServiceOperator) bool)
}

type Option func(*cluster)

type ServiceOperator interface {
	ServiceID() string
	ServiceType() metadata.ServiceType
	Index() int
	Adjust(func(*ServiceConfig))

	Start() error
	Close() error
}
