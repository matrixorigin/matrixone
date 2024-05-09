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
UNAME_S := $(shell uname -s)
GOPATH := $(shell go env GOPATH)
GO_VERSION=$(shell go version)
BRANCH_NAME=$(shell git rev-parse --abbrev-ref HEAD)
LAST_COMMIT_ID=$(shell git rev-parse --short HEAD)
BUILD_TIME=$(shell date +%s)
MO_VERSION=$(shell git describe --always --contains $(shell git rev-parse HEAD))
GO_MODULE=$(shell go list -m)
MUSL_DIR=$(ROOT_DIR)/musl
MUSL_CC=$(MUSL_DIR)/bin/musl-gcc
MUSL_VERSION:=1.2.5

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

RACE_OPT :=
DEBUG_OPT :=
CGO_DEBUG_OPT :=
CGO_OPTS=CGO_CFLAGS="-I$(ROOT_DIR)/cgo " CGO_LDFLAGS="-L$(ROOT_DIR)/cgo -lm -lmo"
GOLDFLAGS=-ldflags="-X '$(GO_MODULE)/pkg/version.GoVersion=$(GO_VERSION)' -X '$(GO_MODULE)/pkg/version.BranchName=$(BRANCH_NAME)' -X '$(GO_MODULE)/pkg/version.CommitID=$(LAST_COMMIT_ID)' -X '$(GO_MODULE)/pkg/version.BuildTime=$(BUILD_TIME)' -X '$(GO_MODULE)/pkg/version.Version=$(MO_VERSION)'"
TAGS :=

.PHONY: cgo
cgo:
	@(cd cgo; ${MAKE} ${CGO_DEBUG_OPT})

# build mo-service binary
.PHONY: build
build: config cgo
	$(info [Build binary])
	$(CGO_OPTS) go build $(TAGS) $(RACE_OPT) $(GOLDFLAGS) $(DEBUG_OPT) -o $(BIN_NAME) ./cmd/mo-service

.PHONY: musl-install
musl-install:
ifeq ("$(UNAME_S)","Linux")
 ifeq ("$(wildcard $(MUSL_CC))","")
	@rm -rf /tmp/musl-$(MUSL_VERSION) musl-$(MUSL_VERSION).tar.gz
	@curl -SfL "https://musl.libc.org/releases/musl-$(MUSL_VERSION).tar.gz" -o /tmp/musl-$(MUSL_VERSION).tar.gz
	@tar -zxf /tmp/musl-$(MUSL_VERSION).tar.gz -C $(ROOT_DIR)
	@cd musl-$(MUSL_VERSION) && ./configure --prefix=$(MUSL_DIR) --syslibdir=$(MUSL_DIR)/syslib && $(MAKE) && $(MAKE) install
	@rm -rf musl-$(MUSL_VERSION) /tmp/musl-$(MUSL_VERSION).tar.gz
 endif
endif

.PHONY: musl-cgo
musl-cgo: musl-install
	@(cd $(ROOT_DIR)/cgo; CC=$(MUSL_CC) ${MAKE} ${CGO_DEBUG_OPT})

.PHONY: musl
musl: override CGO_OPTS += CC=$(MUSL_CC)
musl: override GOLDFLAGS:=-ldflags="--linkmode 'external' --extldflags '-static' -X '$(GO_MODULE)/pkg/version.GoVersion=$(GO_VERSION)' -X '$(GO_MODULE)/pkg/version.BranchName=$(BRANCH_NAME)' -X '$(GO_MODULE)/pkg/version.CommitID=$(LAST_COMMIT_ID)' -X '$(GO_MODULE)/pkg/version.BuildTime=$(BUILD_TIME)' -X '$(GO_MODULE)/pkg/version.Version=$(MO_VERSION)'"
musl: override TAGS := -tags musl
musl: musl-install musl-cgo config
musl:
	$(info [Build binary(musl)])
	$(CGO_OPTS) go build $(TAGS) $(RACE_OPT) $(GOLDFLAGS) $(DEBUG_OPT) -o $(BIN_NAME) ./cmd/mo-service

# build mo-debug tool
.PHONY: mo-debug
mo-debug: config cgo
	$(info [Build mo-debug tool])
	$(CGO_OPTS) go build -o mo-debug ./cmd/mo-debug

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

COMPOSE_LAUNCH := "launch-multi-cn"

.PHONY: compose
compose:
	@cd optools/compose_bvt && ./compose_bvt.sh $(ROOT_DIR) $(COMPOSE_LAUNCH)

.PHONY: compose-clean
compose-clean:
	@docker compose -f etc/launch-tae-compose/compose.yaml --profile $(COMPOSE_LAUNCH) down --remove-orphans
	@docker volume rm launch-tae-compose_minio_storage
	@docker image prune -f

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
	rm -rf /tmp/musl-$(MUSL_VERSION).tar.gz
	$(MAKE) -C cgo clean

###############################################################################
# static checks
###############################################################################

.PHONY: fmt
fmt:
	gofmt -l -s -w .

.PHONY: install-static-check-tools
install-static-check-tools:
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | bash -s -- -b $(GOPATH)/bin v1.55.2
	@go install github.com/matrixorigin/linter/cmd/molint@latest
	@go install github.com/apache/skywalking-eyes/cmd/license-eye@v0.4.0

.PHONY: static-check
static-check: config cgo err-check
	$(CGO_OPTS) go vet -vettool=`which molint` ./...
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