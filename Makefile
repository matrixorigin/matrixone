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
#	make build
#
# To re-build MO in debug mode also with race detector enabled -
#
# make clean
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
BIN_NAME := mo-service
UNAME_S := $(shell uname -s | tr A-Z a-z)
UNAME_M := $(shell uname -m)
GOPATH := $(shell go env GOPATH)
GO_VERSION=$(shell go version)
BRANCH_NAME=$(shell git rev-parse --abbrev-ref HEAD)
LAST_COMMIT_ID=$(shell git rev-parse --short HEAD)
BUILD_TIME=$(shell date +%s)
MO_VERSION=$(shell git symbolic-ref -q --short HEAD || git describe --tags --exact-match)
GO_MODULE=$(shell go list -m)

# check the MUSL_TARGET from https://musl.cc
# make MUSL_TARGET=aarch64-linux musl to cross make the aarch64 linux executable
ifeq ($(MUSL_TARGET),)
	MUSL_TARGET=$(UNAME_M)-$(UNAME_S)
	#MUSL_TARGET=x86_64-linux
endif
MUSL_NAME=$(MUSL_TARGET)-musl-cross
MUSL_DIR=$(ROOT_DIR)/$(MUSL_NAME)
MUSL_TAR=$(MUSL_NAME).tgz
MUSL_CC=$(MUSL_DIR)/bin/$(MUSL_TARGET)-musl-gcc
MUSL_CXX=$(MUSL_DIR)/bin/$(MUSL_TARGET)-musl-g++

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

THIRDPARTIES_INSTALL_DIR=$(ROOT_DIR)/thirdparties/install
RACE_OPT :=
DEBUG_OPT :=
CGO_DEBUG_OPT :=
CGO_OPTS :=CGO_CFLAGS="-I$(THIRDPARTIES_INSTALL_DIR)/include"
GOLDFLAGS=-ldflags="-extldflags '-L$(THIRDPARTIES_INSTALL_DIR)/lib -Wl,-rpath,$(THIRDPARTIES_INSTALL_DIR)/lib' -X '$(GO_MODULE)/pkg/version.GoVersion=$(GO_VERSION)' -X '$(GO_MODULE)/pkg/version.BranchName=$(BRANCH_NAME)' -X '$(GO_MODULE)/pkg/version.CommitID=$(LAST_COMMIT_ID)' -X '$(GO_MODULE)/pkg/version.BuildTime=$(BUILD_TIME)' -X '$(GO_MODULE)/pkg/version.Version=$(MO_VERSION)'"
TAGS :=

ifeq ($(GOBUILD_OPT),)
	GOBUILD_OPT :=
endif

.PHONY: cgo
cgo:
	@(cd cgo; ${MAKE} ${CGO_DEBUG_OPT})

.PHONY: thirdparties
thirdparties:
	@(cd thirdparties; ${MAKE})

# build mo-service binary
.PHONY: build
build: config cgo thirdparties
	$(info [Build binary])
	$(CGO_OPTS) go build $(TAGS) $(RACE_OPT) $(GOLDFLAGS) $(DEBUG_OPT) $(GOBUILD_OPT) -o $(BIN_NAME) ./cmd/mo-service

# https://wiki.musl-libc.org/getting-started.html
# https://musl.cc/
.PHONY: musl-install
musl-install:
ifeq ("$(UNAME_S)","linux")
 ifeq ("$(wildcard $(MUSL_CC))","")
	@rm -rf /tmp/$(MUSL_TAR)
	@echo "https://musl.cc/${MUSL_TAR}"
	@curl -SfL "https://musl.cc/$(MUSL_TAR)" -o /tmp/$(MUSL_TAR)
	@tar -zxf /tmp/$(MUSL_TAR) -C $(ROOT_DIR)
 endif
endif

.PHONY: musl-cgo
musl-cgo: musl-install
	@(cd $(ROOT_DIR)/cgo; CC=$(MUSL_CC) ${MAKE} ${CGO_DEBUG_OPT})


musl-thirdparties: musl-install
	@(cd $(ROOT_DIR)/thirdparties; MUSL=ON CC=$(MUSL_CC) CXX=$(MUSL_CXX) ${MAKE} ${CGO_DEBUG_OPT})
	
.PHONY: musl
musl: override CGO_OPTS += CC=$(MUSL_CC)
musl: override GOLDFLAGS:=-ldflags="--linkmode 'external' --extldflags '-static -L$(THIRDPARTIES_INSTALL_DIR)/lib -lstdc++ -Wl,-rpath,$(THIRDPARTIES_INSTALL_DIR)/lib' -X '$(GO_MODULE)/pkg/version.GoVersion=$(GO_VERSION)' -X '$(GO_MODULE)/pkg/version.BranchName=$(BRANCH_NAME)' -X '$(GO_MODULE)/pkg/version.CommitID=$(LAST_COMMIT_ID)' -X '$(GO_MODULE)/pkg/version.BuildTime=$(BUILD_TIME)' -X '$(GO_MODULE)/pkg/version.Version=$(MO_VERSION)'"
musl: override TAGS := -tags musl
musl: musl-install musl-cgo config musl-thirdparties
musl:
	$(info [Build binary(musl)])
	$(CGO_OPTS) go build $(TAGS) $(RACE_OPT) $(GOLDFLAGS) $(DEBUG_OPT) $(GOBUILD_OPT) -o $(BIN_NAME) ./cmd/mo-service

# build mo-tool
.PHONY: mo-tool
mo-tool: config cgo thirdparties
	$(info [Build mo-tool tool])
	$(CGO_OPTS) go build $(GOLDFLAGS) -o mo-tool ./cmd/mo-tool

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
ut: config cgo thirdparties
	$(info [Unit testing])
ifeq ($(UNAME_S),darwin)
	@cd optools && ./run_ut.sh UT $(SKIP_TEST)
else
	@cd optools && timeout 60m ./run_ut.sh UT $(SKIP_TEST)
endif

###############################################################################
# bvt and unit test
###############################################################################
UT_PARALLEL ?= 1
ENABLE_UT ?= "false"
GOPROXY ?= "https://proxy.golang.com.cn,direct"
LAUNCH ?= "launch"

.PHONY: ci
ci:
	@rm -rf $(ROOT_DIR)/tester-log
	@docker image prune -f
	@docker build -f optools/bvt_ut/Dockerfile . -t matrixorigin/matrixone:local-ci --build-arg GOPROXY=$(GOPROXY)
	@docker run --name tester -it \
			-e LAUNCH=$(LAUNCH) \
			-e UT_PARALLEL=$(UT_PARALLEL) \
			-e ENABLE_UT=$(ENABLE_UT)\
 			--rm -v $(ROOT_DIR)/tester-log:/matrixone-test/tester-log matrixorigin/matrixone:local-ci

.PHONY: ci-clean
ci-clean:
	@docker rmi matrixorigin/matrixone:local-ci
	@docker image prune -f


###############################################################################
# docker compose bvt test
###############################################################################

COMPOSE_LAUNCH := "launch"

.PHONY: compose
compose:
	@cd optools/compose_bvt && ./compose_bvt.sh $(ROOT_DIR) $(COMPOSE_LAUNCH)

.PHONY: compose-clean
compose-clean:
	@docker compose -f etc/launch-tae-compose/compose.yaml --profile $(COMPOSE_LAUNCH) down --remove-orphans
	@docker volume rm -f launch-tae-compose_minio_storage
	@docker image prune -f
	@cd $(ROOT_DIR) && rm -rf docker-compose-log && rm -rf test/distributed/resources/json/export*
	@cd $(ROOT_DIR) && rm -rf test/distributed/resources/into_outfile/*.csv
	@cd $(ROOT_DIR) && rm -rf test/distributed/resources/into_outfile_2/*.csv

###############################################################################
# clean
###############################################################################

.PHONY: clean
clean:
	$(info [Clean up])
	$(info Clean go test cache)
	@go clean -testcache
	rm -f $(BIN_NAME)
	rm -rf $(ROOT_DIR)/vendor
	rm -rf $(MUSL_DIR)
	rm -rf /tmp/$(MUSL_TAR)
	$(MAKE) -C cgo clean
	$(MAKE) -C thirdparties clean

###############################################################################
# static checks
###############################################################################

.PHONY: fmt
fmt:
	gofmt -l -s -w .

.PHONY: install-static-check-tools
install-static-check-tools:
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | bash -s -- -b $(GOPATH)/bin v1.60.2
	@go install github.com/matrixorigin/linter/cmd/molint@latest
	@go install github.com/apache/skywalking-eyes/cmd/license-eye@v0.4.0

.PHONY: static-check
static-check: config err-check
	$(CGO_OPTS) go vet -vettool=`which molint` ./...
	$(CGO_OPTS) license-eye -c .licenserc.yml header check
	$(CGO_OPTS) license-eye -c .licenserc.yml dep check
	$(CGO_OPTS) golangci-lint run -c .golangci.yml ./...

fmtErrs := $(shell grep -onr 'fmt.Errorf' pkg/ --exclude-dir=.git --exclude-dir=vendor \
				--exclude=*.pb.go --exclude=system_vars.go --exclude=Makefile)
errNews := $(shell grep -onr 'errors.New' pkg/ --exclude-dir=.git --exclude-dir=vendor \
				--exclude=*.pb.go --exclude=system_vars.go --exclude=Makefile)
withTimeout := $(shell grep -onr 'context.WithTimeout' pkg/ --exclude-dir=.git --exclude-dir=vendor \
				--exclude=*.pb.go --exclude=*_test.go --exclude=system_vars.go --exclude=Makefile)
withDeadline := $(shell grep -onr 'context.WithDeadline' pkg/ --exclude-dir=.git --exclude-dir=vendor \
				--exclude=*.pb.go --exclude=*_test.go --exclude=system_vars.go --exclude=Makefile)


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
 ifneq ("$(strip $(withTimeout))", "")
 		$(warning 'context.WithTimeout' is found.)
 		$(warning One of 'context.WithTimeout' is called at: $(shell printf "%s\n" $(errNews) | head -1))
 endif
 ifneq ("$(strip $(withDeadline))", "")
 		$(warning 'context.WithDeadline' is found.)
 		$(warning One of 'context.WithDeadline' is called at: $(shell printf "%s\n" $(errNews) | head -1))
 endif
	$(error Use moerr instead.)
else
	$(info Does not find 'fmt.Errorf()', 'errors.New()','context.WithTimeout' and 'context.WithDeadline')
endif
