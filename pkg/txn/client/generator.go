package client

import (
	"github.com/google/uuid"
)

var _ TxnIDGenerator = (*uuidTxnIDGenerator)(nil)

type uuidTxnIDGenerator struct {
}

func newUUIDTxnIDGenerator() TxnIDGenerator {
	return &uuidTxnIDGenerator{}
}

func (gen *uuidTxnIDGenerator) Generate() []byte {
	id := uuid.New()
	return id[:]
}
