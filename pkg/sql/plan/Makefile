CURRENT_DIR = $(shell pwd)

.PHONY: generate_mock
generate_mock:
	@go install github.com/golang/mock/mockgen@v1.6.0
	@echo "Current Directory " $(CURRENT_DIR)
	@mockgen -source=../../../pkg/sql/plan/types.go -package plan -mock_names CompilerContext=MockCompilerContext2,Optimizer=MockOptimizer2  > types_mock.go

