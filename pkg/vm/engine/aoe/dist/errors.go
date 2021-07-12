package dist

import "errors"

var (
	ErrCMDNotSupport = errors.New("command is not support")
	ErrMarshalFailed = errors.New("request marshal has failed")
	ErrInvalidValue  = errors.New("value is invalid")
	ErrShardNotExisted = errors.New("shard is not existed")
	ErrDispatchFailed = errors.New("dispath raft query failed")
)

func errorResp(err error) []byte {
	return []byte(err.Error())
}
