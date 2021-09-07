# This Makefile is to build MatrixOne

BIN_NAME := mo-server
BUILD_CFG := gen_config
UNAME_S := $(shell uname -s)

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
build: cmd/db-server/main.go
	$(info [Build binary])
	@go build -o $(BIN_NAME) cmd/db-server/main.go

# Building mo-server binary for debugging, it uses the latest MatrixCube from master.
.PHONY: debug
debug: cmd/db-server/main.go
	$(info [Build binary for debug])
	go get github.com/matrixorigin/matrixcube
	go mod tidy
	go build -tags debug -o $(BIN_NAME) cmd/db-server/main.go

# Run Static Code Analysis
.PHONY: sca
sca:
	$(info [Static code analysis])
	@cd optools && ./run_ut.sh SCA

# Excluding frontend test cases temporarily
# Argument SKIP_TEST to skip a specific go test
.PHONY: ut 
ut:
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
	@cd optools; ./run_bvt.sh BVT True
else
	@cd optools; timeout 30m ./run_bvt.sh BVT True
endif

# Tear down
.PHONY: clean
clean:
	$(info [Clean up])
	$(info Clean go test cache)
	@go clean -testcache
ifneq ($(wildcard $(BIN_NAME)),)
	$(info Remove file $(BIN_NAME))
	@rm -f $(BIN_NAME)
endif
ifneq ($(wildcard $(BUILD_CFG)),)
	$(info Remove file $(BUILD_CFG))
	@rm -f $(BUILD_CFG)
endif

