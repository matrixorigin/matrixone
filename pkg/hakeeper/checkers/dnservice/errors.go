package dnservice

import "fmt"

var (
	errShardNotExist  = fmt.Errorf("shard not exist")
	errNoWrokingStore = fmt.Errorf("no working store")
)
