package catalogkv

type KVArgs struct {
	Op   uint64   `json:"op"`
	Args [][]byte `json:"args,omitempty"`
}
