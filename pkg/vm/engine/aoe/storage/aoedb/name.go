package aoedb

import "strconv"

func ShardIdToName(id uint64) string {
	return strconv.FormatUint(id, 10)
}

func NameToShardId(name string) (uint64, error) {
	return strconv.ParseUint(name, 10, 64)
}
