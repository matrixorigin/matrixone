package aoedb

import "strconv"

var IdToNameFactory = new(idToNameFactory)

type DBNameFactory interface {
	Encode(interface{}) string
	Decode(string) interface{}
}

type idToNameFactory struct{}

func (f *idToNameFactory) Encode(v interface{}) string {
	shardId := v.(uint64)
	return strconv.FormatUint(shardId, 10)
}

func (f *idToNameFactory) Decode(name string) (interface{}, error) {
	return strconv.ParseUint(name, 10, 64)
}
