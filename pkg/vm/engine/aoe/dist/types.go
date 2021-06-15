package dist

type Args struct {
	Op      uint64   `json:"op"`
	Args    [][]byte `json:"args,omitempty"`
	Limit   uint64   `json:"limit"`
	ShardId uint64   `json:"shard_id"`
}
