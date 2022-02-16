# This Makefile is to build MatrixOne

BIN_NAME := mo-server
BUILD_CFG := gen_config
UNAME_S := $(shell uname -s)
GO_VERSION=$(shell go version)
BRANCH_NAME=$(shell git rev-parse --abbrev-ref HEAD)
LAST_COMMIT_ID=$(shell git rev-parse HEAD)
BUILD_TIME=$(shell date)
MO_Version=$(shell git describe --abbrev=0 --tags)
TARGET_OS ?= $(shell echo $(UNAME_S) | awk '{print tolower($0)}')
TARGET_ARCH ?= $(shell arch)

# generate files generated from .template and needs to delete when clean
GENERATE_OVERLOAD_LOGIC := ./pkg/sql/colexec/extend/overload/and.go ./pkg/sql/colexec/extend/overload/or.go
GENERATE_OVERLOAD_MATH := ./pkg/sql/colexec/extend/overload/div.go ./pkg/sql/colexec/extend/overload/minus.go ./pkg/sql/colexec/extend/overload/mod.go ./pkg/sql/colexec/extend/overload/plus.go ./pkg/sql/colexec/extend/overload/mult.go 
GENERATE_OVERLOAD_COMPARE := ./pkg/sql/colexec/extend/overload/eq.go ./pkg/sql/colexec/extend/overload/ge.go ./pkg/sql/colexec/extend/overload/ne.go /pkg/sql/colexec/extend/overload/ge.go ./pkg/sql/colexec/extend/overload/gt.go ./pkg/sql/colexec/extend/overload/le.go ./pkg/sql/colexec/extend/overload/lt.go
GENERATE_OVERLOAD_OTHERS := ./pkg/sql/colexec/extend/overload/like.go ./pkg/sql/colexec/extend/overload/cast.go
GENERATE_OVERLOAD_UNARYS := ./pkg/sql/colexec/extend/overload/unaryops.go

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

# Building mo-server binary
.PHONY: build
build: cmd/db-server/$(wildcard *.go)
	@go generate ./pkg/sql/colexec/extend/overload
	$(info [Build binary])
	@CGO_ENABLED=1 GOOS=$(TARGET_OS) GOARCH=$(TARGET_ARCH) go build -ldflags="-X 'main.GoVersion=$(GO_VERSION)' -X 'main.BranchName=$(BRANCH_NAME)' -X 'main.LastCommitId=$(LAST_COMMIT_ID)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.MoVersion=$(MO_Version)'" -o $(BIN_NAME) ./cmd/db-server/


# Building mo-server binary for debugging, it uses the latest MatrixCube from master.
.PHONY: debug
debug: cmd/db-server/$(wildcard *.go)
	@go generate ./pkg/sql/colexec/extend/overload
	$(info [Build binary for debug])
	go get github.com/matrixorigin/matrixcube
	go mod tidy
	go build -ldflags="-X 'main.GoVersion=$(GO_VERSION)' -X 'main.BranchName=$(BRANCH_NAME)' -X 'main.LastCommitId=$(LAST_COMMIT_ID)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.MoVersion=$(MO_Version)'" -o $(BIN_NAME) ./cmd/db-server/


# Run Static Code Analysis
.PHONY: sca
sca:
	@go generate ./pkg/sql/colexec/extend/overload
	$(info [Static code analysis])
	@cd optools && ./run_ut.sh SCA

# Excluding frontend test cases temporarily
# Argument SKIP_TEST to skip a specific go test
.PHONY: ut 
ut:
	@go generate ./pkg/sql/colexec/extend/overload
	$(info [Unit testing])
ifeq ($(UNAME_S),Darwin)
	@cd optools && ./run_ut.sh UT $(SKIP_TEST)
else
	@cd optools && timeout 30m ./run_ut.sh UT $(SKIP_TEST)
endif

# Running build verification tests
.PHONY: bvt
bvt: mo-server
	$(info [Build verification testing])
ifeq ($(UNAME_S),Darwin)
	@cd optools; ./run_bvt.sh BVT False
else
	@cd optools; timeout 30m ./run_bvt.sh BVT False
endif

# Tear down
.PHONY: clean
clean:
	$(info [Clean up])
	$(info Clean go test cache)
	@go clean -testcache
	@rm -f $(GENERATE_OVERLOAD_LOGIC)
	@rm -f $(GENERATE_OVERLOAD_MATH)
	@rm -f $(GENERATE_OVERLOAD_COMPARE)
	@rm -f $(GENERATE_OVERLOAD_OTHERS)
	@rm -f $(GENERATE_OVERLOAD_UNARYS)
ifneq ($(wildcard $(BIN_NAME)),)
	$(info Remove file $(BIN_NAME))
	@rm -f $(BIN_NAME)
endif
ifneq ($(wildcard $(BUILD_CFG)),)
	$(info Remove file $(BUILD_CFG))
	@rm -f $(BUILD_CFG)
endif
