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
# To construct a directory named vendor in the main module’s root directory that contains copies of all packages needed to support builds and tests of packages in the main module. 
# make vendor
#

# where am I
ROOT_DIR = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
GOBIN := go
BIN_NAME := mo-server
SERVICE_BIN_NAME := mo-service
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
# build vendor directory 
###############################################################################

VENDOR_DIRECTORY := ./vendor
.PHONY: vendor-build
vendor-build: 
	$(info [go mod vendor])
	@go mod vendor



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
pb: vendor-build generate-pb fmt
	$(info all protos are generated) 

###############################################################################
# build mo-server
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
	$(GO) build $(RACE_OPT) $(GOLDFLAGS) -o $(BIN_NAME) ./cmd/db-server

# build mo-server binary for debugging with go's race detector enabled
# produced executable is 10x slower and consumes much more memory
.PHONY: debug
debug: override BUILD_NAME := debug-binary
debug: override RACE_OPT := -race
debug: build

###############################################################################
# build mo-service
###############################################################################

# build mo-service binary
.PHONY: service
service: config cgo cmd/mo-service/$(wildcard *.go)
	$(info [Build $(BUILD_NAME)])
	$(GO) build $(RACE_OPT) $(GOLDFLAGS) -o $(SERVICE_BIN_NAME) ./cmd/mo-service

.PHONY: debug-service
debug-service: override BUILD_NAME := debug-binary
debug-service: override RACE_OPT := -race
debug-service: service


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
	rm -f $(CONFIG_CODE_GENERATED) $(BIN_NAME) $(SERVICE_BIN_NAME) $(BUILD_CFG)
	rm -rf $(VENDOR_DIRECTORY)
	$(MAKE) -C cgo clean

###############################################################################
# static checks
###############################################################################

.PHONY: fmt
fmt:
	gofmt -l -s -w .

.PHONY: install-static-check-tools
install-static-check-tools:
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | bash -s -- -b $(GOPATH)/bin v1.47.2
	@go install github.com/matrixorigin/linter/cmd/molint@latest
	@go install github.com/apache/skywalking-eyes/cmd/license-eye@v0.4.0


.PHONY: static-check
static-check: config cgo
	$(CGO_OPTS) go vet -vettool=`which molint` ./...
	$(CGO_OPTS) license-eye -c .licenserc.yml header check
	$(CGO_OPTS) license-eye -c .licenserc.yml dep check
	$(CGO_OPTS) golangci-lint run -c .golangci.yml ./...


