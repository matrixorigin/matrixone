# Copyright 2021 - 2022 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Examples
#
# By default, make builds the mo-server
#
# make
#
# To re-build MO -
#	
#	make clean
#	make config
#	make build
#
# To re-build MO in debug mode also with race detector enabled -
#
# make clean
# make config
# make debug
#
# To run static checks - 
# 
# make install-static-check-tools
# make static-check
#

# where am I
ROOT_DIR = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
GOBIN := go
BIN_NAME := mo-server
BUILD_CFG := gen_config
UNAME_S := $(shell uname -s)
GOPATH := $(shell go env GOPATH)
GO_VERSION=$(shell go version)
BRANCH_NAME=$(shell git rev-parse --abbrev-ref HEAD)
LAST_COMMIT_ID=$(shell git rev-parse HEAD)
BUILD_TIME=$(shell date)
MO_VERSION=$(shell git describe --tags $(shell git rev-list --tags --max-count=1))

# cross compilation has been disabled for now
ifneq ($(GOARCH)$(TARGET_ARCH)$(GOOS)$(TARGET_OS),)
$(error cross compilation has been disabled)
endif

###############################################################################
# default target
###############################################################################
all: build

###############################################################################
# code generation
###############################################################################

# files generated from cmd/generate-config
# they need to be deleted in the clean target
CONFIG_CODE_GENERATED := ./pkg/config/system_vars.go ./pkg/config/system_vars_test.go

CONFIG_DEPS=cmd/generate-config/main.go  \
	cmd/generate-config/config_template.go \
	cmd/generate-config/system_vars_def.toml

.PHONY: config
config: $(CONFIG_DEPS)
	$(info [Create build config])
	@go mod tidy
	@go build -o $(BUILD_CFG) cmd/generate-config/main.go cmd/generate-config/config_template.go
	@./$(BUILD_CFG) cmd/generate-config/system_vars_def.toml
	@mv -f cmd/generate-config/system_vars_config.toml .
	@mv -f cmd/generate-config/system_vars.go pkg/config
	@mv -f cmd/generate-config/system_vars_test.go pkg/config

.PHONY: generate-pb
generate-pb:
	$(ROOT_DIR)/proto/gen.sh

# Generate protobuf files
.PHONY: pb
pb: generate-pb fmt
	$(info all protos are generated) 

###############################################################################
# build MatrixOne
###############################################################################

RACE_OPT := 
CGO_OPTS=CGO_CFLAGS="-I$(ROOT_DIR)/cgo" CGO_LDFLAGS="-L$(ROOT_DIR)/cgo -lmo"
GO=$(CGO_OPTS) $(GOBIN)
GOLDFLAGS=-ldflags="-X 'main.GoVersion=$(GO_VERSION)' -X 'main.BranchName=$(BRANCH_NAME)' -X 'main.LastCommitId=$(LAST_COMMIT_ID)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.MoVersion=$(MO_VERSION)'"

.PHONY: cgo
cgo:
	@(cd cgo; make)

BUILD_NAME=binary
# build mo-server binary
.PHONY: build
build: config cgo cmd/db-server/$(wildcard *.go)
	$(info [Build $(BUILD_NAME)])
	$(GO) build $(RACE_OPT) $(GOLDFLAGS) -o $(BIN_NAME) ./cmd/db-server/

# build mo-server binary for debugging with go's race detector enabled
# produced executable is 10x slower and consumes much more memory
.PHONY: debug
debug: override BUILD_NAME := debug-binary
debug: override RACE_OPT := -race
debug: build

###############################################################################
# run unit tests
###############################################################################
# Excluding frontend test cases temporarily
# Argument SKIP_TEST to skip a specific go test
.PHONY: ut 
ut: config cgo
	$(info [Unit testing])
ifeq ($(UNAME_S),Darwin)
	@cd optools && ./run_ut.sh UT $(SKIP_TEST)
else
	@cd optools && timeout 60m ./run_ut.sh UT $(SKIP_TEST)
endif

###############################################################################
# clean
###############################################################################

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
	$(MAKE) -C cgo clean

###############################################################################
# static checks
###############################################################################

.PHONY: fmt
fmt:
	gofmt -l -s -w .

.PHONY: install-static-check-tools
install-static-check-tools:
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | bash -s -- -b $(GOPATH)/bin v1.47.1
	@go install github.com/matrixorigin/linter/cmd/molint@latest
	@go install github.com/apache/skywalking-eyes/cmd/license-eye@latest
	@go install honnef.co/go/tools/cmd/staticcheck@latest

EXTRA_LINTERS=-E exportloopref -E rowserrcheck -E depguard -D unconvert \
	-E prealloc -E gofmt

STATICCHECK_CHECKS=QF1001,QF1002,QF1003,QF1004,QF1005,QF1006,QF1007,QF1008,QF1009,QF1010,QF1011,QF1012,S1000,S1001,S1002,S1003,S1004,S1005,S1006,S1007,S1008,S1009,S1010,S1011,S1012,S1016,S1017,S1018,S1019,S1020,S1021,S1023,S1024,S1025,S1028,S1029,S1030,S1031,S1032,S1033,S1034,S1035,S1036,S1037,S1038,S1039,S1040,SA1000,SA1001,SA1002,SA1003,SA1004,SA1005,SA1006,SA1007,SA1008,SA1010,SA1011,SA1012,SA1013,SA1014,SA1015,SA1016,SA1017,SA1018,SA1019,SA1020,SA1021,SA1023,SA1024,SA1025,SA1026,SA1027,SA1028,SA1029,SA1030,SA2000,SA2001,SA2002,SA2003,SA3000,SA3001,SA4000,SA4001,SA4003,SA4004,SA4005,SA4006,SA4008,SA4009,SA4010,SA4011,SA4012,SA4013,SA4014,SA4015,SA4016,SA4017,SA4018,SA4019,SA4020,SA4021,SA4022,SA4023,SA4024,SA4025,SA4026,SA4027,SA4028,SA4029,SA4030,SA4031,SA5000,SA5001,SA5002,SA5003,SA5004,SA5005,SA5007,SA5008,SA5009,SA5010,SA5011,SA5012,SA6000,SA6001,SA6002,SA6003,SA6005,SA9001,SA9002,SA9003,SA9004,SA9005,SA9006,SA9007,SA9008,ST1001,ST1005,ST1006,ST1008,ST1011,ST1012,ST1013,ST1015,ST1016,ST1017,ST1018,ST1019,ST1023

.PHONY: static-check
static-check: config cgo
	$(CGO_OPTS) staticcheck -checks $(STATICCHECK_CHECKS) ./...
	$(CGO_OPTS) go vet -vettool=`which molint` ./...
	$(CGO_OPTS) license-eye -c .licenserc.yml header check
	$(CGO_OPTS) license-eye -c .licenserc.yml dep check
	$(CGO_OPTS) golangci-lint run $(EXTRA_LINTERS) ./...
