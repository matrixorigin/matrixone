CURRENT_DIR = $(shell pwd)

.PHONY: generate_mock
generate_mock:
	@go install github.com/golang/mock/mockgen@v1.6.0
	@echo "Current Directory " $(CURRENT_DIR)
	@mockgen -source ../../../pkg/txn/client/types.go -package mock_frontend > txn_mock.go
	@mockgen -source ../../../pkg/vm/engine/types.go -package mock_frontend > engine_mock.go
	@mockgen -source ../types.go -package mock_frontend > types_mock.go

