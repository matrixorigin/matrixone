package catalogkv

import "errors"

var (
	ErrCMDNotSupport = errors.New("command is not support")
	ErrMarshalFailed = errors.New("request marshal has failed")
)
