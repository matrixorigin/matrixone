package file

import (
	"errors"
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var (
	ErrInvalidParam = errors.New("tae: invalid param")
)

type Base interface {
	common.IRef
	io.Closer
	Fingerprint() *common.ID
}
