package dist

type Group uint64

const (
	KVGroup Group = iota
	AOEGroup
)

type Args struct {
	Op    uint64   `json:"op"`
	Args  [][]byte `json:"args,omitempty"`
	Limit uint64   `json:"limit"`
}
