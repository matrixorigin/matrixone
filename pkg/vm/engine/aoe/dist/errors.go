package dist

import (
	"bytes"
	"errors"
	"matrixone/pkg/vm/engine/aoe/common/codec"
)

var (
	ErrCMDNotSupport   = errors.New("command is not support")
	ErrMarshalFailed   = errors.New("request marshal has failed")
	ErrInvalidValue    = errors.New("value is invalid")
	ErrShardNotExisted = errors.New("shard is not existed")
	ErrDispatchFailed  = errors.New("dispath raft query failed")
	ErrKeyNotExisted   = errors.New("request key is not existed")
)

func errorResp(err error, infos ...string) []byte {
	buf := bytes.Buffer{}
	for _, info := range infos {
		buf.WriteString(info)
		buf.WriteString(",")
	}
	buf.Write(codec.String2Bytes(err.Error()))
	return buf.Bytes()
}
