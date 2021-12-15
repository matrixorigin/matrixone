package kv

import "errors"

var (
	NotExist = errors.New("not exist")
)

type KV struct {
	mp map[string][]byte
}
