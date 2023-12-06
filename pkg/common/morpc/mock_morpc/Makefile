CURRENT_DIR = $(shell pwd)

.PHONY: generate_mock
generate_mock:
	@go install github.com/golang/mock/mockgen@v1.6.0
	@echo "Current Directory " $(CURRENT_DIR)
	@mockgen -source ../types.go -package mock_morpc > types_mock.go

