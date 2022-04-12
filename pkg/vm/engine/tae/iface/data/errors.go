package data

import "errors"

var (
	ErrAppendableSegmentNotFound = errors.New("tae: no appendable segment")
	ErrAppendableBlockNotFound   = errors.New("tae: no appendable block")
	ErrNotAppendable             = errors.New("tae: not appendable")
)
