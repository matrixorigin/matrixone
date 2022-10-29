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
# By default, make builds the mo-service
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
BIN_NAME := mo-service
MO_DUMP := mo-dump
BUILD_CFG := gen_config
UNAME_S := $(shell uname -s)
GOPATH := $(shell go env GOPATH)
GO_VERSION=$(shell go version)
BRANCH_NAME=$(shell git rev-parse --abbrev-ref HEAD)
LAST_COMMIT_ID=$(shell git rev-parse HEAD)
BUILD_TIME=$(shell date)
MO_VERSION=$(shell git describe --always --tags $(shell git rev-list --tags --max-count=1))

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

.PHONY: config
config:
	$(info [Create build config])
	@go mod tidy

.PHONY: generate-pb
generate-pb:
	$(ROOT_DIR)/proto/gen.sh

# Generate protobuf files
.PHONY: pb
pb: vendor-build generate-pb fmt
	$(info all protos are generated) 

###############################################################################
# build mo-service
###############################################################################

RACE_OPT := 
DEBUG_OPT := 
CGO_DEBUG_OPT :=
CGO_OPTS=CGO_CFLAGS="-I$(ROOT_DIR)/cgo" CGO_LDFLAGS="-L$(ROOT_DIR)/cgo -lmo -lm"
GO=$(CGO_OPTS) $(GOBIN)
GOLDFLAGS=-ldflags="-X 'main.GoVersion=$(GO_VERSION)' -X 'main.BranchName=$(BRANCH_NAME)' -X 'main.CommitID=$(LAST_COMMIT_ID)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.Version=$(MO_VERSION)'"

.PHONY: cgo
cgo:
	@(cd cgo; make ${CGO_DEBUG_OPT})

BUILD_NAME=binary
# build mo-service binary
.PHONY: build
build: config cgo cmd/mo-service/$(wildcard *.go)
	$(info [Build $(BUILD_NAME)])
	$(GO) build $(RACE_OPT) $(GOLDFLAGS) -o $(BIN_NAME) ./cmd/mo-service
	$(GO) build $(RACE_OPT) $(GOLDFLAGS) -o $(MO_DUMP) ./cmd/mo-dump

.PHONY: mo-dump
mo-dump:
	$(GO) build $(RACE_OPT) $(GOLDFLAGS) -o $(MO_DUMP) ./cmd/mo-dump

# build mo-service binary for debugging with go's race detector enabled
# produced executable is 10x slower and consumes much more memory
.PHONY: debug
debug: override BUILD_NAME := debug-binary
debug: override RACE_OPT := -race
debug: override DEBUG_OPT := -gcflags=all="-N -l"
debug: override CGO_DEBUG_OPT := debug
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
	rm -f $(BIN_NAME) $(BUILD_CFG)
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
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | bash -s -- -b $(GOPATH)/bin v1.48.0
	@go install github.com/matrixorigin/linter/cmd/molint@latest
	@go install github.com/apache/skywalking-eyes/cmd/license-eye@v0.4.0


.PHONY: static-check
static-check: config cgo err-check
	$(CGO_OPTS) go vet -vettool=`which molint` $(shell go list ./... | grep -v github.com/matrixorigin/matrixone/cmd/mo-dump)
	$(CGO_OPTS) license-eye -c .licenserc.yml header check
	$(CGO_OPTS) license-eye -c .licenserc.yml dep check
	$(CGO_OPTS) golangci-lint run -c .golangci.yml ./...

fmtErrs := $(shell grep -onr 'fmt.Errorf' pkg/ --exclude-dir=.git --exclude-dir=vendor \
				--exclude=*.pb.go --exclude=system_vars.go --exclude=Makefile)
errNews := $(shell grep -onr 'errors.New' pkg/ --exclude-dir=.git --exclude-dir=vendor \
				--exclude=*.pb.go --exclude=system_vars.go --exclude=Makefile)

.PHONY: err-check
err-check:
ifneq ("$(strip $(fmtErrs))$(strip $(errNews))", "")
 ifneq ("$(strip $(fmtErrs))", "")
		$(warning 'fmt.Errorf()' is found.)
		$(warning One of 'fmt.Errorf()' is called at: $(shell printf "%s\n" $(fmtErrs) | head -1))
 endif
 ifneq ("$(strip $(errNews))", "")
		$(warning 'errors.New()' is found.)
		$(warning One of 'errors.New()' is called at: $(shell printf "%s\n" $(errNews) | head -1))
 endif

	$(error Use moerr instead.)
else
	$(info Neither 'fmt.Errorf()' nor 'errors.New()' is found)
endif
