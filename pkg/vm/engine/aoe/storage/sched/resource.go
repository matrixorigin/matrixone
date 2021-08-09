package sched

type ResourceType uint16

const (
	ResT_Invalid ResourceType = iota
	ResT_IO
	ResT_CPU
	ResT_Meta
	ResT_Data
)

type BaseResource struct {
	EventHandler
	t    ResourceType
	name string
}

func NewBaseResource(name string, t ResourceType, handler EventHandler) *BaseResource {
	r := &BaseResource{
		t:            t,
		name:         name,
		EventHandler: handler,
	}
	return r
}

func (r *BaseResource) Type() ResourceType { return r.t }
func (r *BaseResource) Name() string       { return r.name }
