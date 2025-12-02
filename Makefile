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
# To compile mo-service with GPU support,
# 1. install CUDA toolkit (version 1.30 or above)
# 2. install cuVS Go bindings with conda
#  % git clone git@github.com:rapidsai/cuvs.git
#  % cd cuvs
#  % conda env create --name go -f conda/environments/go_cuda-130_arch-$(uname -m).yaml
#  % conda activate go
#  % cd matrixone
#  % MO_CL_CUDA=1 make

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
# help
###############################################################################

.PHONY: help
help:
	@echo "MatrixOne Makefile Commands"
	@echo "============================"
	@echo ""
	@echo "Build Commands:"
	@echo "  make build              - Build mo-service binary"
	@echo "  make debug              - Build with race detector and debug symbols"
	@echo "  make musl               - Build static binary with musl"
	@echo "  make mo-tool            - Build mo-tool utility"
	@echo "  make clean              - Clean build artifacts"
	@echo ""
	@echo "Testing:"
	@echo "  make ut                 - Run unit tests"
	@echo "  make ci                 - Run CI tests (BVT + optional UT)"
	@echo "  make compose            - Run docker compose BVT tests"
	@echo ""
	@echo "Development Environment (Local Multi-CN Cluster):"
	@echo "  make dev-help           - Show all dev-* commands (full list)"
	@echo "  make dev-build          - Build docker image"
	@echo "  make dev-up             - Start multi-CN cluster (default: local image)"
	@echo "  make dev-up-latest      - Start with official latest image"
	@echo "  make dev-up-test        - Start with test directory mounted"
	@echo "  make dev-down           - Stop cluster"
	@echo "  make dev-restart        - Restart all services"
	@echo "  make dev-restart-cn1    - Restart CN1 only (also: cn2, proxy, tn, log)"
	@echo "  make dev-edit-cn1       - Edit CN1 config interactively (also: cn2, proxy, tn, log, common)"
	@echo "  make dev-logs           - View all logs"
	@echo "  make dev-mysql          - Connect to database via proxy"
	@echo "  make dev-clean          - Stop and remove all data"
	@echo ""
	@echo "Code Quality:"
	@echo "  make fmt                - Format Go code"
	@echo "  make static-check       - Run static analysis"
	@echo ""
	@echo "Other:"
	@echo "  make vendor-build       - Build vendor directory"
	@echo "  make pb                 - Generate protobuf files"
	@echo ""
	@echo "For more details:"
	@echo "  make dev-help           - Development environment commands"
	@echo "  See README.md and BUILD.md for full documentation"

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

VERSION_INFO :=-X '$(GO_MODULE)/pkg/version.GoVersion=$(GO_VERSION)' -X '$(GO_MODULE)/pkg/version.BranchName=$(BRANCH_NAME)' -X '$(GO_MODULE)/pkg/version.CommitID=$(LAST_COMMIT_ID)' -X '$(GO_MODULE)/pkg/version.BuildTime=$(BUILD_TIME)' -X '$(GO_MODULE)/pkg/version.Version=$(MO_VERSION)'
THIRDPARTIES_INSTALL_DIR=$(ROOT_DIR)/thirdparties/install
RACE_OPT :=
DEBUG_OPT :=
CGO_DEBUG_OPT :=
TAGS :=

ifeq ($(MO_CL_CUDA),1)
	ifeq ($(CONDA_PREFIX),)
		$(error CONDA_PREFIX env variable not found.)
	endif
	CUVS_CFLAGS := -I$(CONDA_PREFIX)/include
	CUVS_LDFLAGS := -L$(CONDA_PREFIX)/envs/go/lib -lcuvs -lcuvs_c
	CUDA_CFLAGS := -I/usr/local/cuda/include $(CUVS_CFLAGS)
	CUDA_LDFLAGS := -L/usr/local/cuda/lib64/stubs -lcuda -L/usr/local/cuda/lib64 -lcudart $(CUVS_LDFLAGS) -lstdc++
	TAGS += -tags "gpu"
endif

CGO_OPTS :=CGO_CFLAGS="-I$(THIRDPARTIES_INSTALL_DIR)/include $(CUDA_CFLAGS)"
GOLDFLAGS=-ldflags="-extldflags '$(CUDA_LDFLAGS) -L$(THIRDPARTIES_INSTALL_DIR)/lib -Wl,-rpath,\$${ORIGIN}/lib -fopenmp' $(VERSION_INFO)"

ifeq ("$(UNAME_S)","darwin")
GOLDFLAGS:=-ldflags="-extldflags '-L$(THIRDPARTIES_INSTALL_DIR)/lib -Wl,-rpath,@executable_path/lib' $(VERSION_INFO)"
endif

ifeq ($(GOBUILD_OPT),)
	GOBUILD_OPT :=
endif

.PHONY: cgo
cgo:
	@(cd cgo; ${MAKE} ${CGO_DEBUG_OPT})

.PHONY: thirdparties
thirdparties:
	@(cd thirdparties; ${MAKE})
	cp -r $(THIRDPARTIES_INSTALL_DIR)/lib $(ROOT_DIR)/

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
musl: override GOLDFLAGS:=-ldflags="--linkmode 'external' --extldflags '-static -L$(THIRDPARTIES_INSTALL_DIR)/lib -lstdc++ -Wl,-rpath,\$${ORIGIN}/lib' $(VERSION_INFO)"
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
# Local Multi-CN Development Environment (docker-multi-cn-local-disk)
###############################################################################

DEV_DIR := etc/docker-multi-cn-local-disk
DEV_VERSION ?= local
DEV_MOUNT ?=

.PHONY: dev-help
dev-help:
	@echo "Local Multi-CN Development Environment Commands:"
	@echo "  make dev-build          - Build MatrixOne docker image (local tag)"
	@echo "  make dev-up             - Start multi-CN cluster with local image"
	@echo "  make dev-up-latest      - Start multi-CN cluster with latest official image"
	@echo "  make dev-up-test        - Start with test directory mounted"
	@echo "  make dev-down           - Stop multi-CN cluster"
	@echo "  make dev-restart        - Restart multi-CN cluster (all services)"
	@echo "  make dev-restart-cn1    - Restart CN1 only"
	@echo "  make dev-restart-cn2    - Restart CN2 only"
	@echo "  make dev-restart-proxy  - Restart Proxy only"
	@echo "  make dev-restart-log    - Restart Log service only"
	@echo "  make dev-restart-tn     - Restart TN only"
	@echo "  make dev-ps             - Show service status"
	@echo "  make dev-logs           - Show all logs (tail -f)"
	@echo "  make dev-logs-cn1       - Show CN1 logs"
	@echo "  make dev-logs-cn2       - Show CN2 logs"
	@echo "  make dev-logs-proxy     - Show proxy logs"
	@echo "  make dev-clean          - Stop and remove all data (WARNING: destructive!)"
	@echo "  make dev-config         - Generate config from config.env"
	@echo "  make dev-config-example - Create config.env.example file"
	@echo "  make dev-edit-cn1       - Edit CN1 configuration interactively"
	@echo "  make dev-edit-cn2       - Edit CN2 configuration interactively"
	@echo "  make dev-edit-proxy     - Edit Proxy configuration interactively"
	@echo "  make dev-edit-log       - Edit Log service configuration interactively"
	@echo "  make dev-edit-tn        - Edit TN configuration interactively"
	@echo "  make dev-edit-common    - Edit common configuration (all services)"
	@echo ""
	@echo "Examples:"
	@echo "  make dev-build && make dev-up              # Build and start"
	@echo "  make dev-up-test                           # Start with test files"
	@echo "  make DEV_VERSION=latest dev-up             # Use official latest"
	@echo "  make DEV_VERSION=nightly dev-up            # Use nightly build"
	@echo "  make DEV_MOUNT='../../test:/test:ro' dev-up  # Custom mount"
	@echo ""
	@echo "Configuration (Choose one method):"
	@echo "  Method 1 (Interactive - Recommended):"
	@echo "    make dev-edit-cn1                        # Edit CN1 config in editor"
	@echo "    (Remove # to enable settings, save to apply)"
	@echo "  Method 2 (Manual):"
	@echo "    1. Copy: cp $(DEV_DIR)/config.env.example $(DEV_DIR)/config.env"
	@echo "    2. Edit: vim $(DEV_DIR)/config.env (uncomment and modify)"
	@echo "    3. Generate: make dev-config (or auto-generated on dev-up)"

.PHONY: dev-build
dev-build:
	@echo "Building MatrixOne docker image..."
	@cd $(DEV_DIR) && ./start.sh build

.PHONY: dev-up
dev-up:
	@echo "Starting MatrixOne Multi-CN cluster (version: $(DEV_VERSION))..."
ifeq ($(DEV_MOUNT),)
	@cd $(DEV_DIR) && ./start.sh -v $(DEV_VERSION) up -d
else
	@cd $(DEV_DIR) && ./start.sh -v $(DEV_VERSION) -m "$(DEV_MOUNT)" up -d
endif
	@echo ""
	@echo "Services started! Connect with:"
	@echo "  mysql -h 127.0.0.1 -P 6001 -u root -p111  # Via proxy (recommended)"
	@echo "  mysql -h 127.0.0.1 -P 16001 -u root -p111  # Direct to CN1"
	@echo "  mysql -h 127.0.0.1 -P 16002 -u root -p111  # Direct to CN2"

.PHONY: dev-up-latest
dev-up-latest:
	@$(MAKE) DEV_VERSION=latest dev-up

.PHONY: dev-up-nightly
dev-up-nightly:
	@$(MAKE) DEV_VERSION=nightly dev-up

.PHONY: dev-up-test
dev-up-test:
	@echo "Starting with test directory mounted..."
	@cd $(DEV_DIR) && ./start.sh -v $(DEV_VERSION) -m "../../test:/test:ro" up -d
	@echo ""
	@echo "Test directory mounted at /test in containers"
	@echo "Run SQL files with: mysql> source /test/distributed/cases/your_test.sql;"

.PHONY: dev-down
dev-down:
	@echo "Stopping MatrixOne Multi-CN cluster..."
	@cd $(DEV_DIR) && ./start.sh down

.PHONY: dev-restart
dev-restart:
	@echo "Restarting MatrixOne Multi-CN cluster..."
	@cd $(DEV_DIR) && ./start.sh restart

# Restart individual services
.PHONY: dev-restart-cn1
dev-restart-cn1:
	@echo "Restarting CN1..."
	@cd $(DEV_DIR) && docker compose restart mo-cn1

.PHONY: dev-restart-cn2
dev-restart-cn2:
	@echo "Restarting CN2..."
	@cd $(DEV_DIR) && docker compose restart mo-cn2

.PHONY: dev-restart-proxy
dev-restart-proxy:
	@echo "Restarting Proxy..."
	@cd $(DEV_DIR) && docker compose restart mo-proxy

.PHONY: dev-restart-log
dev-restart-log:
	@echo "Restarting Log service..."
	@cd $(DEV_DIR) && docker compose restart mo-log

.PHONY: dev-restart-tn
dev-restart-tn:
	@echo "Restarting TN..."
	@cd $(DEV_DIR) && docker compose restart mo-tn

.PHONY: dev-ps
dev-ps:
	@cd $(DEV_DIR) && ./start.sh ps

.PHONY: dev-logs
dev-logs:
	@cd $(DEV_DIR) && ./start.sh logs -f

.PHONY: dev-logs-cn1
dev-logs-cn1:
	@cd $(DEV_DIR) && ./start.sh logs -f mo-cn1

.PHONY: dev-logs-cn2
dev-logs-cn2:
	@cd $(DEV_DIR) && ./start.sh logs -f mo-cn2

.PHONY: dev-logs-proxy
dev-logs-proxy:
	@cd $(DEV_DIR) && ./start.sh logs -f mo-proxy

.PHONY: dev-logs-tn
dev-logs-tn:
	@cd $(DEV_DIR) && ./start.sh logs -f mo-tn

.PHONY: dev-logs-log
dev-logs-log:
	@cd $(DEV_DIR) && ./start.sh logs -f mo-log

.PHONY: dev-clean
dev-clean:
	@echo "WARNING: This will delete all data in mo-data/ and logs/!"
	@read -p "Continue? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		echo "Stopping services..."; \
		cd $(DEV_DIR) && ./start.sh down; \
		echo "Removing data..."; \
		rm -rf mo-data logs; \
		echo "Clean completed!"; \
	else \
		echo "Cancelled."; \
	fi

.PHONY: dev-shell-cn1
dev-shell-cn1:
	@docker exec -it mo-cn1 sh

.PHONY: dev-shell-cn2
dev-shell-cn2:
	@docker exec -it mo-cn2 sh

.PHONY: dev-mysql
dev-mysql:
	@echo "Connecting to MatrixOne via proxy..."
	@mysql -h 127.0.0.1 -P 6001 -u root -p111

.PHONY: dev-mysql-cn1
dev-mysql-cn1:
	@echo "Connecting to MatrixOne CN1..."
	@mysql -h 127.0.0.1 -P 16001 -u root -p111

.PHONY: dev-mysql-cn2
dev-mysql-cn2:
	@echo "Connecting to MatrixOne CN2..."
	@mysql -h 127.0.0.1 -P 16002 -u root -p111

.PHONY: dev-config
dev-config:
	@echo "Generating configuration files..."
	@cd $(DEV_DIR) && ./generate-config.sh

.PHONY: dev-config-example
dev-config-example:
	@echo "Creating example config.env file..."
	@if [ -f "$(DEV_DIR)/config.env" ]; then \
		echo "Warning: config.env already exists. Not overwriting."; \
		echo "To recreate, run: rm $(DEV_DIR)/config.env && make dev-config-example"; \
	else \
		cp $(DEV_DIR)/config.env.example $(DEV_DIR)/config.env; \
		echo "✓ Created $(DEV_DIR)/config.env"; \
		echo ""; \
		echo "Edit the file to customize configuration:"; \
		echo "  vim $(DEV_DIR)/config.env"; \
		echo ""; \
		echo "Then regenerate configs:"; \
		echo "  make dev-config"; \
		echo ""; \
		echo "Or just restart (auto-generates):"; \
		echo "  make dev-down && make dev-up"; \
	fi

# Interactive configuration editors for specific services
.PHONY: dev-edit-cn1
dev-edit-cn1:
	@cd $(DEV_DIR) && ./edit-config.sh cn1

.PHONY: dev-edit-cn2
dev-edit-cn2:
	@cd $(DEV_DIR) && ./edit-config.sh cn2

.PHONY: dev-edit-proxy
dev-edit-proxy:
	@cd $(DEV_DIR) && ./edit-config.sh proxy

.PHONY: dev-edit-log
dev-edit-log:
	@cd $(DEV_DIR) && ./edit-config.sh log

.PHONY: dev-edit-tn
dev-edit-tn:
	@cd $(DEV_DIR) && ./edit-config.sh tn

.PHONY: dev-edit-common
dev-edit-common:
	@cd $(DEV_DIR) && ./edit-config.sh common

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
	rm -rf $(ROOT_DIR)/lib

###############################################################################
# static checks
###############################################################################

.PHONY: fmt
fmt:
	gofmt -l -s -w .

.PHONY: install-static-check-tools
install-static-check-tools:
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | bash -s -- -b $(GOPATH)/bin v2.6.2
	@go install github.com/matrixorigin/linter/cmd/molint@latest
	@go install github.com/apache/skywalking-eyes/cmd/license-eye@v0.4.0

.PHONY: static-check
static-check: config err-check
	$(CGO_OPTS) go vet -vettool=`which molint` ./...
	$(CGO_OPTS) license-eye -c .licenserc.yml header check
	$(CGO_OPTS) license-eye -c .licenserc.yml dep check
	$(CGO_OPTS) golangci-lint run -v -c .golangci.yml ./...

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
