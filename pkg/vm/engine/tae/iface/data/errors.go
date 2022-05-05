package data

import "errors"

var (
	ErrAppendableSegmentNotFound = errors.New("tae: no appendable segment")
	ErrAppendableBlockNotFound   = errors.New("tae: no appendable block")
	ErrNotAppendable             = errors.New("tae: not appendable")
	ErrStaleRequest              = errors.New("tae: stale request")

	ErrPossibleDuplicate = errors.New("tae: possible duplicate")
	ErrDuplicate         = errors.New("tae: duplicate")
)
