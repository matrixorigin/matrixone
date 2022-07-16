# This Makefile is to build MatrixOne
ROOT_DIR = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

BIN_NAME := mo-server
BUILD_CFG := gen_config
UNAME_S := $(shell uname -s)
GOPATH := $(shell go env GOPATH)
GO_VERSION=$(shell go version)
BRANCH_NAME=$(shell git rev-parse --abbrev-ref HEAD)
LAST_COMMIT_ID=$(shell git rev-parse HEAD)
BUILD_TIME=$(shell date)
MO_Version=$(shell git describe --abbrev=0 --tags)
TARGET_OS ?=
TARGET_ARCH ?=
BVT_BRANCH ?= master

# files generated from cmd/generate-config
# they need to be deleted in cleaning
CONFIG_CODE_GENERATED := ./pkg/config/system_vars.go ./pkg/config/system_vars_test.go

# Creating build config
.PHONY: config
config: cmd/generate-config/main.go cmd/generate-config/config_template.go cmd/generate-config/system_vars_def.toml
	$(info [Create build config])
	@go mod tidy
	@go build -o $(BUILD_CFG) cmd/generate-config/main.go cmd/generate-config/config_template.go
	@./$(BUILD_CFG) cmd/generate-config/system_vars_def.toml
	@mv -f cmd/generate-config/system_vars_config.toml .
	@mv -f cmd/generate-config/system_vars.go pkg/config
	@mv -f cmd/generate-config/system_vars_test.go pkg/config

.PHONY: generate

.PHONY: generate-pb
generate-pb:
	$(ROOT_DIR)/proto/gen.sh

# Generate protobuf files
.PHONY: pb
pb: generate-pb fmt
	$(info all protos are generated) 

.PHONY: cgo
cgo:
	@(cd cgo; make)

# Building mo-server binary
.PHONY: build
build: generate cgo cmd/db-server/$(wildcard *.go)
ifeq ($(TARGET_OS)$(TARGET_ARCH), )
	$(info [Build binary])
	@CGO_CFLAGS="-I$(ROOT_DIR)/cgo" CGO_LDFLAGS="-L$(ROOT_DIR)/cgo -lmo" go build -ldflags="-X 'main.GoVersion=$(GO_VERSION)' -X 'main.BranchName=$(BRANCH_NAME)' -X 'main.LastCommitId=$(LAST_COMMIT_ID)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.MoVersion=$(MO_Version)'" -o $(BIN_NAME) ./cmd/db-server/
else ifneq ($(TARGET_OS), )
ifneq ($(TARGET_ARCH), )
	$(info [Cross Build binary])
	$(info $(TARGET_OS))
	$(info $(TARGET_ARCH))
	@CGO_ENABLED=1 CGO_CFLAGS="-I$(ROOT_DIR)/cgo" CGO_LDFLAGS="-L$(ROOT_DIR)/cgo -lmo" GOOS=$(TARGET_OS) GOARCH=$(TARGET_ARCH) go build -ldflags="-X 'main.GoVersion=$(GO_VERSION)' -X 'main.BranchName=$(BRANCH_NAME)' -X 'main.LastCommitId=$(LAST_COMMIT_ID)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.MoVersion=$(MO_Version)'" -o $(BIN_NAME) ./cmd/db-server/
else
	$(info [Cross Build binary])
	$(info $(TARGET_OS))
	@CGO_ENABLED=1 CGO_CFLAGS="-I$(ROOT_DIR)/cgo" CGO_LDFLAGS="-L$(ROOT_DIR)/cgo -lmo" GOOS=$(TARGET_OS) go build -ldflags="-X 'main.GoVersion=$(GO_VERSION)' -X 'main.BranchName=$(BRANCH_NAME)' -X 'main.LastCommitId=$(LAST_COMMIT_ID)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.MoVersion=$(MO_Version)'" -o $(BIN_NAME) ./cmd/db-server/
endif
endif

# Building mo-server binary for debugging, "race detector" enabled.
.PHONY: debug
debug: generate cgo cmd/db-server/$(wildcard *.go)
ifeq ($(TARGET_OS)$(TARGET_ARCH), )
	$(info [Build debug binary])
	@CGO_CFLAGS="-I$(ROOT_DIR)/cgo" CGO_LDFLAGS="-L$(ROOT_DIR)/cgo -lmo" go build -race -ldflags="-X 'main.GoVersion=$(GO_VERSION)' -X 'main.BranchName=$(BRANCH_NAME)' -X 'main.LastCommitId=$(LAST_COMMIT_ID)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.MoVersion=$(MO_Version)'" -o $(BIN_NAME) ./cmd/db-server/
else ifneq ($(TARGET_OS), )
ifneq ($(TARGET_ARCH), )
	$(info [Cross Build  debugbinary])
	$(info $(TARGET_OS))
	$(info $(TARGET_ARCH))
	@CGO_ENABLED=1 CGO_CFLAGS="-I$(ROOT_DIR)/cgo" CGO_LDFLAGS="-L$(ROOT_DIR)/cgo -lmo" GOOS=$(TARGET_OS) GOARCH=$(TARGET_ARCH) go build  -race -ldflags="-X 'main.GoVersion=$(GO_VERSION)' -X 'main.BranchName=$(BRANCH_NAME)' -X 'main.LastCommitId=$(LAST_COMMIT_ID)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.MoVersion=$(MO_Version)'" -o $(BIN_NAME) ./cmd/db-server/
else
	$(info [Cross Build debug binary])
	$(info $(TARGET_OS))
	@CGO_ENABLED=1 CGO_CFLAGS="-I$(ROOT_DIR)/cgo" CGO_LDFLAGS="-L$(ROOT_DIR)/cgo -lmo" GOOS=$(TARGET_OS) go build  -race -ldflags="-X 'main.GoVersion=$(GO_VERSION)' -X 'main.BranchName=$(BRANCH_NAME)' -X 'main.LastCommitId=$(LAST_COMMIT_ID)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.MoVersion=$(MO_Version)'" -o $(BIN_NAME) ./cmd/db-server/
endif
endif

# Excluding frontend test cases temporarily
# Argument SKIP_TEST to skip a specific go test
.PHONY: ut 
ut: generate cgo
	$(info [Unit testing])
ifeq ($(UNAME_S),Darwin)
	@cd optools && ./run_ut.sh UT $(SKIP_TEST)
else
	@cd optools && timeout 60m ./run_ut.sh UT $(SKIP_TEST)
endif

# Tear down
.PHONY: clean
clean:
	$(info [Clean up])
	$(info Clean go test cache)
	@go clean -testcache
	@rm -f $(CONFIG_CODE_GENERATED)
ifneq ($(wildcard $(BIN_NAME)),)
	$(info Remove file $(BIN_NAME))
	@rm -f $(BIN_NAME)
endif
ifneq ($(wildcard $(BUILD_CFG)),)
	$(info Remove file $(BUILD_CFG))
	@rm -f $(BUILD_CFG)
endif

###############################################################################
# static checks
###############################################################################

.PHONY: fmt
fmt:
	gofmt -l -s -w .


.PHONY: install-static-check-tools
install-static-check-tools:
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | bash -s -- -b $(GOPATH)/bin v1.45.2
	@go install github.com/matrixorigin/linter/cmd/molint@latest
	@go install github.com/google/go-licenses@latest
	@go install honnef.co/go/tools/cmd/staticcheck@latest

# TODO: tracking https://github.com/golangci/golangci-lint/issues/2649
DIRS=pkg/... \
	 cmd/...

EXTRA_LINTERS=-E exportloopref -E rowserrcheck -E depguard -D unconvert \
	-E prealloc -E gofmt -E stylecheck

.PHONY: static-check
static-check: generate cgo
	@CGO_CFLAGS="-I$(ROOT_DIR)/cgo" CGO_LDFLAGS="-L$(ROOT_DIR)/cgo -lmo" staticcheck -checks SA9001,S1009,S1023,S1002,S1025,ST1017,ST1019,S1019,S1004,SA1030,S1011,S1008,SA1024,SA4003,S1039,S1028,SA6005 ./...
	@CGO_CFLAGS="-I$(ROOT_DIR)/cgo" CGO_LDFLAGS="-L$(ROOT_DIR)/cgo -lmo" go vet -vettool=$(which molint) ./...
	@CGO_CFLAGS="-I$(ROOT_DIR)/cgo" CGO_LDFLAGS="-L$(ROOT_DIR)/cgo -lmo" go-licenses check ./...
	@for p in $(DIRS); do \
    CGO_CFLAGS="-I$(ROOT_DIR)/cgo" CGO_LDFLAGS="-L$(ROOT_DIR)/cgo -lmo" golangci-lint run $(EXTRA_LINTERS) --new-from-rev=HEAD~ $$p ; \
  done;
