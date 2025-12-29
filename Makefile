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
# 3. compile matrixone
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
	@echo "  make build-typecheck    - Build with typecheck enabled (enables type checking)"
	@echo "  make build TYPECHECK=1  - Build with typecheck enabled (alternative)"
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
	@echo "Local Development with MinIO:"
	@echo "  make dev-up-minio-local     - Start MinIO service (local storage)"
	@echo "  make dev-down-minio-local   - Stop MinIO service"
	@echo "  make dev-restart-minio-local - Restart MinIO service"
	@echo "  make dev-status-minio-local - Show MinIO service status"
	@echo "  make dev-logs-minio-local   - Show MinIO logs"
	@echo "  make dev-clean-minio-local  - Clean MinIO data"
	@echo "  make launch-minio           - Build and start MO with MinIO storage"
	@echo "  make launch-minio-debug     - Build (debug) and start MO with MinIO"
	@echo ""
	@echo "Development Environment (Local Multi-CN Cluster):"
	@echo "  make dev-help           - Show all dev-* commands (full list)"
	@echo "  make dev-build          - Build docker image (smart cache - rebuilds when code changes)"
	@echo "  make dev-build-force    - Build docker image (forces complete rebuild, no cache)"
	@echo "  make dev-up             - Start multi-CN cluster (default: local image)"
	@echo "  make dev-up-latest      - Start with official latest image"
	@echo "  make dev-up-test        - Start with test directory mounted"
	@echo "  make dev-up-grafana-local - Start Grafana dashboard (port 3001)"
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

ifeq ($(TYPECHECK),1)
	TAGS += -tags "typecheck"
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

# build mo-service binary with typecheck enabled
# enables type checking for ToSliceNoTypeCheck and ToSliceNoTypeCheck2
.PHONY: build-typecheck
build-typecheck: override TYPECHECK := 1
build-typecheck: build

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
	@echo "  make dev-build          - Build MatrixOne docker image (typecheck enabled by default)"
	@echo "  make dev-build TYPECHECK=0 - Build without typecheck (for performance testing)"
	@echo "  make dev-build-force    - Force rebuild (typecheck enabled by default)"
	@echo "  make dev-build-force TYPECHECK=0 - Force rebuild without typecheck"
	@echo "  make dev-up             - Start multi-CN cluster with local image (with NET_ADMIN for network chaos)"
	@echo "  make dev-up-latest      - Start multi-CN cluster with latest official image"
	@echo "  make dev-up-test        - Start with test directory mounted"
	@echo "  make dev-up-grafana-local - Start Grafana dashboard (port 3001)"
	@echo "  make dev-down           - Stop multi-CN cluster"
	@echo "  make dev-restart        - Restart multi-CN cluster (all services, preserves NET_ADMIN)"
	@echo "  make dev-restart-cn1    - Restart CN1 only"
	@echo "  make dev-restart-cn2    - Restart CN2 only"
	@echo "  make dev-restart-proxy  - Restart Proxy only"
	@echo "  make dev-restart-log    - Restart Log service only"
	@echo "  make dev-restart-tn     - Restart TN only"
	@echo "  make dev-up-grafana-local - Start Grafana dashboard (port 3001)"
	@echo "  make dev-down-grafana-local - Stop Grafana dashboard"
	@echo "  make dev-ps             - Show service status"
	@echo "  make dev-check-grafana - Check if Grafana is ready (port 3001)"
	@echo "  make dev-logs           - Show all logs (tail -f)"
	@echo "  make dev-logs-cn1       - Show CN1 logs"
	@echo "  make dev-logs-cn2       - Show CN2 logs"
	@echo "  make dev-logs-proxy     - Show proxy logs"
	@echo "  make dev-logs-grafana-local - Show Grafana logs"
	@echo "  make dev-clean          - Stop and remove all data (WARNING: destructive!)"
	@echo "  make dev-cleanup        - Interactive cleanup (stops containers, removes data directories)"
	@echo "  make dev-config         - Generate config from config.env (default: check-fraction=1000)"
	@echo "  make dev-config-example - Create config.env.example file"
	@echo "  make dev-setup-docker-mirror - Configure Docker registry mirror (for faster pulls)"
	@echo "  make dev-edit-cn1       - Edit CN1 configuration interactively"
	@echo "  make dev-edit-cn2       - Edit CN2 configuration interactively"
	@echo "  make dev-edit-proxy     - Edit Proxy configuration interactively"
	@echo "  make dev-edit-log       - Edit Log service configuration interactively"
	@echo "  make dev-edit-tn        - Edit TN configuration interactively"
	@echo "  make dev-edit-common    - Edit common configuration (all services)"
	@echo "  make dev-setup-docker-mirror - Configure Docker registry mirror (for faster pulls)"
	@echo ""
	@echo "Dashboard Management (via mo-tool):"
	@echo "  Dashboard commands (default: local mode, port 3001):"
	@echo "    make dev-create-dashboard        - Create dashboards"
	@echo "    make dev-list-dashboard          - List dashboards"
	@echo "    make dev-delete-dashboard        - Delete all dashboards in folder"
	@echo ""
	@echo "  Custom options:"
	@echo "    DASHBOARD_MODE=cloud DASHBOARD_PORT=3000 make dev-create-dashboard"
	@echo "    DASHBOARD_MODE=k8s DASHBOARD_PORT=3001 make dev-create-dashboard"
	@echo "    DASHBOARD_HOST=localhost DASHBOARD_PORT=3001 make dev-create-dashboard"
	@echo ""
	@echo "  Or use mo-tool directly:"
	@echo "    ./mo-tool dashboard create --mode local --port 3001"
	@echo "    ./mo-tool dashboard list --mode cloud --port 3000"
	@echo "    ./mo-tool dashboard delete --mode local --port 3001          # Delete all dashboards in folder"
	@echo "    ./mo-tool dashboard delete-dashboard --uid <uid>             # Delete single dashboard by UID"
	@echo "    ./mo-tool dashboard delete-folder --mode local --port 3001   # Delete entire folder and all dashboards"
	@echo ""
	@echo "Examples:"
	@echo "  make dev-build && make dev-up              # Build and start"
	@echo "  make dev-up-test                           # Start with test files"
	@echo "  make dev-up-grafana-local                  # Start Grafana dashboard (port 3001)"
	@echo "  make DEV_VERSION=latest dev-up             # Use official latest"
	@echo "  make DEV_VERSION=nightly dev-up            # Use nightly build"
	@echo "  make DEV_MOUNT='../../test:/test:ro' dev-up  # Custom mount"
	@echo ""
	@echo "Network Chaos Testing:"
	@echo "  Containers (mo-cn1, mo-cn2, mo-tn) are configured with NET_ADMIN capability"
	@echo "  This enables network chaos testing using tc (traffic control) tool"
	@echo ""
	@echo "  Network-only mode (containers keep running):"
	@echo "    make dev-chaos-light          - Light network chaos (10-30ms delay, 1-3% loss)"
	@echo "    make dev-chaos-moderate      - Moderate network chaos (50-150ms delay, 5-10% loss)"
	@echo "    make dev-chaos-severe        - Severe network chaos (200-400ms delay, 15-25% loss)"
	@echo "    make dev-chaos-inter-region  - Inter-region network (100-200ms delay, 2-5% loss)"
	@echo "    make dev-chaos-inter-continent - Inter-continent network (300-500ms delay, 5-10% loss)"
	@echo "    make dev-chaos-congestion     - Network congestion (50-100ms delay, 10-20% loss)"
	@echo "    make dev-chaos-bandwidth     - Bandwidth limitation (10Mbps)"
	@echo "    make dev-chaos-random        - Random delay or loss"
	@echo ""
	@echo "  Custom network chaos:"
	@echo "    make dev-chaos CN=cn1 TYPE=delay DELAY=100    # 100ms delay on cn1 (inject until Ctrl+C)"
	@echo "    make dev-chaos CN=cn2 TYPE=loss LOSS=20        # 20% loss on cn2 (inject until Ctrl+C)"
	@echo "    make dev-chaos CN=cn1,cn2 TYPE=delay DELAY=100  # 100ms delay on cn1 and cn2 (inject until Ctrl+C)"
	@echo "    make dev-chaos CN=cn1,tn TYPE=loss LOSS=20      # 20% loss on cn1 and tn (inject until Ctrl+C)"
	@echo "    make dev-chaos CN=cn1,cn2,tn TYPE=delay DELAY=150  # 150ms delay on all nodes (inject until Ctrl+C)"
	@echo ""
	@echo "  Combined network chaos (delay + loss + bandwidth):"
	@echo "    make dev-chaos CN=cn1 TYPE=combined DELAY=100 LOSS=20 BANDWIDTH=10  # All three effects"
	@echo "    make dev-chaos CN=cn1,cn2 TYPE=combined DELAY=150 LOSS=15 BANDWIDTH=5 DURATION=60  # For 60 seconds"
	@echo ""
	@echo "  Network chaos with duration (auto-restore after duration):"
	@echo "    make dev-chaos CN=cn1 TYPE=delay DELAY=100 DURATION=60    # 100ms delay for 60 seconds"
	@echo "    make dev-chaos CN=cn1,cn2 TYPE=loss LOSS=20 DURATION=120  # 20% loss for 120 seconds"
	@echo "    make dev-chaos CN=cn1,tn TYPE=delay DELAY=150 DURATION=300  # 150ms delay for 5 minutes"
	@echo ""
	@echo "  Use chaos-test.sh script directly for advanced options:"
	@echo "    cd $(DEV_DIR) && ./chaos-test.sh --network-only -c cn1 --scenario moderate"
	@echo ""
	@echo "Grafana Monitoring:"
	@echo "  Access Grafana UI at http://localhost:3000 (admin/admin)"
	@echo "  Note: Enable metrics in service configs (disableMetric = false)"
	@echo ""
	@echo "Configuration (Choose one method):"
	@echo "  Method 1 (Interactive - Recommended):"
	@echo "    make dev-edit-cn1                        # Edit CN1 config in editor"
	@echo "    (Remove # to enable settings, save to apply)"
	@echo "  Method 2 (Manual):"
	@echo "    1. Copy: cp $(DEV_DIR)/config.env.example $(DEV_DIR)/config.env"
	@echo "    2. Edit: vim $(DEV_DIR)/config.env (uncomment and modify)"
	@echo "    3. Generate: make dev-config (or auto-generated on dev-up)"
	@echo ""
	@echo "Memory Allocation Check (check-fraction):"
	@echo "  Default: 1000 (checks 1 in 1000 deallocations for double free/missing free)"
	@echo "  Lower values = more checks (better error detection, higher overhead)"
	@echo "  Higher values = fewer checks (better performance, may miss errors)"
	@echo "  Set CHECK_FRACTION=0 to disable (maximum performance, no error detection)"
	@echo "  Configure in config.env: CHECK_FRACTION=1000"

.PHONY: dev-build
dev-build:
	@echo "Building MatrixOne docker image (using smart cache - only rebuilds when code changes)..."
	@if [ "$(TYPECHECK)" = "0" ]; then \
		echo "Building WITHOUT typecheck (TYPECHECK=0)"; \
		cd $(DEV_DIR) && TYPECHECK=0 ./start.sh build mo-log; \
	else \
		echo "Building WITH typecheck (default, use TYPECHECK=0 to disable)"; \
		cd $(DEV_DIR) && TYPECHECK=1 ./start.sh build mo-log; \
	fi

.PHONY: dev-build-force
dev-build-force:
	@echo "Building MatrixOne docker image (forcing complete rebuild, no cache)..."
	@if [ "$(TYPECHECK)" = "0" ]; then \
		echo "Building WITHOUT typecheck (TYPECHECK=0)"; \
		cd $(DEV_DIR) && TYPECHECK=0 ./start.sh build --no-cache mo-log; \
	else \
		echo "Building WITH typecheck (default, use TYPECHECK=0 to disable)"; \
		cd $(DEV_DIR) && TYPECHECK=1 ./start.sh build --no-cache mo-log; \
	fi

.PHONY: dev-up
dev-up:
	@echo "Starting MatrixOne Multi-CN cluster (version: $(DEV_VERSION))..."
	@echo "Note: Containers (mo-cn1, mo-cn2, mo-tn) are configured with NET_ADMIN capability"
	@echo "      for network chaos testing using tc (traffic control) tool"
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
	@echo ""
	@echo "Network chaos testing:"
	@echo "  cd $(DEV_DIR) && ./chaos-test.sh -c cn1 -n loss --loss 20"

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

.PHONY: dev-up-grafana-local
dev-up-grafana-local:
	@echo "Starting Grafana dashboard for monitoring MatrixOne services..."
	@if [ -n "$(HOST_PROMETHEUS_PORT)" ]; then \
		echo "Using host Prometheus at port $(HOST_PROMETHEUS_PORT)"; \
	fi
	@echo ""
	@echo "Pre-creating directories with correct permissions..."
	@DOCKER_UID=$$(id -u) && \
		DOCKER_GID=$$(id -g) && \
		mkdir -p prometheus-local-data grafana-local-data grafana-local-data/dashboards && \
		chown -R $$DOCKER_UID:$$DOCKER_GID prometheus-local-data grafana-local-data 2>/dev/null || true
	@echo "Checking and fixing ownership for existing directories..."
	@DOCKER_UID=$$(id -u) && \
		DOCKER_GID=$$(id -g) && \
		if [ -d grafana-local-data ] && find grafana-local-data -user root 2>/dev/null | grep -q .; then \
			echo "Fixing ownership for grafana-local-data (found root-owned files)..." && \
			sudo chown -R $$DOCKER_UID:$$DOCKER_GID grafana-local-data 2>/dev/null || \
			echo "Warning: Could not fix ownership. You may need to run: sudo chown -R $$DOCKER_UID:$$DOCKER_GID grafana-local-data"; \
		fi && \
		if [ -d prometheus-local-data ] && find prometheus-local-data -user root 2>/dev/null | grep -q .; then \
			echo "Fixing ownership for prometheus-local-data (found root-owned files)..." && \
			sudo chown -R $$DOCKER_UID:$$DOCKER_GID prometheus-local-data 2>/dev/null || \
			echo "Warning: Could not fix ownership. You may need to run: sudo chown -R $$DOCKER_UID:$$DOCKER_GID prometheus-local-data"; \
		fi
	@cd $(DEV_DIR) && \
		export DOCKER_UID=$$(id -u) && \
		export DOCKER_GID=$$(id -g) && \
		export HOST_PROMETHEUS_PORT=$(HOST_PROMETHEUS_PORT) && \
		./start.sh --profile prometheus-local up -d || \
		(echo "" && \
		 echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" && \
		 echo "❌ Docker pull failed! Please configure Docker mirror:" && \
		 echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" && \
		 echo "" && \
		 echo "Quick fix (recommended):" && \
		 echo "  cd $(DEV_DIR)" && \
		 echo "  sudo ./setup-docker-mirror.sh" && \
		 echo "" && \
		 echo "Or use Makefile command:" && \
		 echo "  sudo make dev-setup-docker-mirror" && \
		 echo "" && \
		 echo "Then retry:" && \
		 echo "  make dev-up-grafana-local" && \
		 echo "" && \
		 exit 1)
	@echo ""
	@echo "✅ Monitoring dashboard started! Access at:"
	@echo "  http://localhost:3001  # Grafana UI (admin/admin)"
	@echo ""
	@if [ -n "$(HOST_PROMETHEUS_PORT)" ]; then \
		echo "Grafana is configured to use host Prometheus at port $(HOST_PROMETHEUS_PORT)"; \
	else \
		echo "Grafana is using internal Prometheus (prometheus-local)"; \
		echo "To use host Prometheus, set HOST_PROMETHEUS_PORT:"; \
		echo "  HOST_PROMETHEUS_PORT=9090 make dev-up-grafana-local"; \
	fi
	@echo ""
	@echo "Make sure your MatrixOne services have metrics enabled:"
	@echo "  [observability]"
	@echo "  disableMetric = false"
	@echo "  enable-metric-to-prom = true"
	@echo "  status-port = 7001"

.PHONY: dev-down
dev-down:
	@echo "Stopping MatrixOne Multi-CN cluster..."
	@cd $(DEV_DIR) && \
		export DOCKER_UID=$$(id -u) && \
		export DOCKER_GID=$$(id -g) && \
		./start.sh --profile matrixone down

.PHONY: dev-restart
dev-restart:
	@echo "Restarting MatrixOne Multi-CN cluster..."
	@echo "Note: NET_ADMIN capability is preserved for network chaos testing"
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

.PHONY: dev-down-grafana-local
dev-down-grafana-local:
	@echo "Stopping and removing local monitoring services (Grafana + Prometheus)..."
	@cd $(DEV_DIR) && docker compose --profile prometheus-local down
	@echo "✅ Monitoring services stopped and removed"

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

.PHONY: dev-logs-grafana-local
dev-logs-grafana-local:
	@cd $(DEV_DIR) && docker compose --profile prometheus-local logs -f grafana-local

.PHONY: dev-check-grafana
dev-check-grafana:
	@cd $(DEV_DIR) && ./start.sh check-grafana

# Network Chaos Testing Commands
.PHONY: dev-chaos dev-chaos-light dev-chaos-moderate dev-chaos-severe dev-chaos-inter-region dev-chaos-inter-continent dev-chaos-congestion dev-chaos-bandwidth dev-chaos-random
dev-chaos:
	@if [ -z "$(CN)" ]; then \
		echo "Error: CN is required. Use: make dev-chaos CN=cn1 TYPE=delay DELAY=100"; \
		echo "  For multiple nodes: make dev-chaos CN=cn1,cn2,tn TYPE=delay DELAY=100"; \
		exit 1; \
	fi
	@if [ -z "$(TYPE)" ]; then \
		echo "Error: TYPE is required. Use: make dev-chaos CN=cn1 TYPE=delay DELAY=100"; \
		echo "  For multiple nodes: make dev-chaos CN=cn1,cn2,tn TYPE=delay DELAY=100"; \
		exit 1; \
	fi
	@cd $(DEV_DIR) && ./chaos-test.sh --network-only -c $(CN) -n $(TYPE) \
		$$([ -n "$(DELAY)" ] && echo "--delay $(DELAY)") \
		$$([ -n "$(LOSS)" ] && echo "--loss $(LOSS)") \
		$$([ -n "$(BANDWIDTH)" ] && echo "--bandwidth $(BANDWIDTH)") \
		$$([ -n "$(DURATION)" ] && echo "--duration $(DURATION)")

dev-chaos-light:
	@cd $(DEV_DIR) && ./chaos-test.sh --network-only -c $(or $(CN),cn1) --scenario light

dev-chaos-moderate:
	@cd $(DEV_DIR) && ./chaos-test.sh --network-only -c $(or $(CN),cn1) --scenario moderate

dev-chaos-severe:
	@cd $(DEV_DIR) && ./chaos-test.sh --network-only -c $(or $(CN),cn1) --scenario severe

dev-chaos-inter-region:
	@cd $(DEV_DIR) && ./chaos-test.sh --network-only -c $(or $(CN),cn1) --scenario inter-region

dev-chaos-inter-continent:
	@cd $(DEV_DIR) && ./chaos-test.sh --network-only -c $(or $(CN),cn1) --scenario inter-continent

dev-chaos-congestion:
	@cd $(DEV_DIR) && ./chaos-test.sh --network-only -c $(or $(CN),cn1) --scenario congestion

dev-chaos-bandwidth:
	@cd $(DEV_DIR) && ./chaos-test.sh --network-only -c $(or $(CN),cn1) --scenario bandwidth

dev-chaos-random:
	@cd $(DEV_DIR) && ./chaos-test.sh --network-only -c $(or $(CN),cn1) --random

.PHONY: dev-setup-docker-mirror
dev-setup-docker-mirror:
	@echo "Setting up Docker registry mirror for faster image pulls..."
	@echo ""
	@if [ "$$(id -u)" -ne 0 ]; then \
		echo "This command needs to be run with sudo:"; \
		echo "  sudo make dev-setup-docker-mirror"; \
		exit 1; \
	fi
	@bash -c 'cd $(DEV_DIR) && ./setup-docker-mirror.sh'

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

.PHONY: dev-cleanup
dev-cleanup:
	@echo "Running interactive cleanup script..."
	@cd $(DEV_DIR) && ./cleanup.sh

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
# Dashboard Creation
###############################################################################

DASHBOARD_PORT ?= 3001
DASHBOARD_HOST ?= 127.0.0.1
DASHBOARD_MODE ?= local
DASHBOARD_USERNAME ?= admin
DASHBOARD_PASSWORD ?= admin
DASHBOARD_DATASOURCE ?= Prometheus

.PHONY: dev-create-dashboard
dev-create-dashboard: mo-tool
	@echo "Creating dashboard..."
	@echo "  Mode: $(DASHBOARD_MODE)"
	@echo "  Host: $(DASHBOARD_HOST):$(DASHBOARD_PORT)"
	@echo "  Username: $(DASHBOARD_USERNAME)"
	@./mo-tool dashboard \
		--host $(DASHBOARD_HOST) \
		--port $(DASHBOARD_PORT) \
		--mode $(DASHBOARD_MODE) \
		--username $(DASHBOARD_USERNAME) \
		--password $(DASHBOARD_PASSWORD) \
		--datasource $(DASHBOARD_DATASOURCE)


# List dashboards
.PHONY: dev-list-dashboard
dev-list-dashboard: mo-tool
	@echo "Listing dashboards..."
	@echo "  Mode: $(DASHBOARD_MODE)"
	@echo "  Host: $(DASHBOARD_HOST):$(DASHBOARD_PORT)"
	@echo "  Username: $(DASHBOARD_USERNAME)"
	@./mo-tool dashboard list \
		--host $(DASHBOARD_HOST) \
		--port $(DASHBOARD_PORT) \
		--mode $(DASHBOARD_MODE) \
		--username $(DASHBOARD_USERNAME) \
		--password $(DASHBOARD_PASSWORD) \
		--datasource $(DASHBOARD_DATASOURCE)

.PHONY: dev-list-dashboard-local
dev-list-dashboard-local:
	@$(MAKE) DASHBOARD_MODE=local DASHBOARD_PORT=3001 dev-list-dashboard

.PHONY: dev-list-dashboard-cluster
dev-list-dashboard-cluster:
	@$(MAKE) DASHBOARD_MODE=cloud DASHBOARD_PORT=3000 DASHBOARD_DATASOURCE=Prometheus dev-list-dashboard

.PHONY: dev-list-dashboard-k8s
dev-list-dashboard-k8s:
	@$(MAKE) DASHBOARD_MODE=k8s DASHBOARD_PORT=$(DASHBOARD_PORT) dev-list-dashboard

.PHONY: dev-list-dashboard-cloud-ctrl
dev-list-dashboard-cloud-ctrl:
	@$(MAKE) DASHBOARD_MODE=cloud-ctrl DASHBOARD_PORT=$(DASHBOARD_PORT) dev-list-dashboard

# Delete dashboards
.PHONY: dev-delete-dashboard
dev-delete-dashboard: mo-tool
	@echo "Deleting dashboards..."
	@echo "  Mode: $(DASHBOARD_MODE)"
	@echo "  Host: $(DASHBOARD_HOST):$(DASHBOARD_PORT)"
	@echo "  Username: $(DASHBOARD_USERNAME)"
	@./mo-tool dashboard delete \
		--host $(DASHBOARD_HOST) \
		--port $(DASHBOARD_PORT) \
		--mode $(DASHBOARD_MODE) \
		--username $(DASHBOARD_USERNAME) \
		--password $(DASHBOARD_PASSWORD) \
		--datasource $(DASHBOARD_DATASOURCE)


###############################################################################
# Local Development with MinIO Storage
###############################################################################

MINIO_DIR := etc/launch-minio-local
MINIO_DATA_DIR := $(MINIO_DIR)/mo-data/minio-data

.PHONY: dev-up-minio-local
dev-up-minio-local:
	@echo "Starting MinIO service for local MatrixOne..."
	@mkdir -p $(MINIO_DATA_DIR)
	@DOCKER_PLATFORM=$$(uname -m | sed 's/x86_64/linux\/amd64/; s/arm64/linux\/arm64/; s/aarch64/linux\/arm64/') && \
		echo "Detected platform: $$DOCKER_PLATFORM" && \
		cd $(MINIO_DIR) && \
		DOCKER_UID=$$(id -u) DOCKER_GID=$$(id -g) DOCKER_PLATFORM=$$DOCKER_PLATFORM docker compose up -d
	@echo ""
	@echo "✅ MinIO started!"
	@echo "  - API: http://127.0.0.1:9000"
	@echo "  - Console: http://127.0.0.1:9001"
	@echo "  - Access Key: minio"
	@echo "  - Secret Key: minio123"
	@echo "  - Data directory: $(MINIO_DATA_DIR)"

.PHONY: dev-down-minio-local
dev-down-minio-local:
	@echo "Stopping MinIO service..."
	@cd $(MINIO_DIR) && docker compose down
	@echo "✅ MinIO stopped"

.PHONY: dev-restart-minio-local
dev-restart-minio-local:
	@echo "Restarting MinIO service..."
	@cd $(MINIO_DIR) && docker compose restart minio
	@echo "✅ MinIO restarted"

.PHONY: dev-status-minio-local
dev-status-minio-local:
	@cd $(MINIO_DIR) && docker compose ps

.PHONY: dev-logs-minio-local
dev-logs-minio-local:
	@cd $(MINIO_DIR) && docker compose logs -f minio

.PHONY: dev-clean-minio-local
dev-clean-minio-local:
	@echo "WARNING: This will delete all MinIO data!"
	@read -p "Continue? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		echo "Stopping MinIO..."; \
		cd $(MINIO_DIR) && docker compose down; \
		echo "Removing data..."; \
		rm -rf $(MINIO_DATA_DIR); \
		echo "✅ MinIO data cleaned!"; \
	else \
		echo "Cancelled."; \
	fi

.PHONY: launch-minio
launch-minio: build dev-up-minio-local
	@echo ""
	@echo "Starting MatrixOne with MinIO storage..."
	@echo "  Launch config: $(MINIO_DIR)/launch.toml"
	@echo ""
	@./mo-service -launch $(MINIO_DIR)/launch.toml

.PHONY: launch-minio-debug
launch-minio-debug: debug dev-up-minio-local
	@echo ""
	@echo "Starting MatrixOne (debug mode) with MinIO storage..."
	@echo "  Launch config: $(MINIO_DIR)/launch.toml"
	@echo ""
	@./mo-service -launch $(MINIO_DIR)/launch.toml

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
				--exclude=*.pb.go --exclude=*_test.go --exclude=system_vars.go --exclude=Makefile)
errNews := $(shell grep -onr 'errors.New' pkg/ --exclude-dir=.git --exclude-dir=vendor \
				--exclude=*.pb.go --exclude=*_test.go --exclude=system_vars.go --exclude=Makefile)
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
		$(warning One of 'context.WithTimeout' is called at: $(shell printf "%s\n" $(withTimeout) | head -1))
 endif
 ifneq ("$(strip $(withDeadline))", "")
		$(warning 'context.WithDeadline' is found.)
		$(warning One of 'context.WithDeadline' is called at: $(shell printf "%s\n" $(withDeadline) | head -1))
 endif
	$(error Use moerr instead.)
else
	$(info Does not find 'fmt.Errorf()', 'errors.New()','context.WithTimeout' and 'context.WithDeadline')
endif
