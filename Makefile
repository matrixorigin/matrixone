# This Makefile is to build MatrixOne

BIN_NAME = mo-server
BUILD_OUT = gen_config
BUILD_LOGS = logs
VET_REPORT = vt_report.txt
UT_REPORT = ut_report.txt

# Creating build config
.PHONY: config
config: cmd/generate-config/main.go cmd/generate-config/config_template.go cmd/generate-config/system_vars_def.toml
	$(info >>> Generating build config)
	@go mod tidy
	@go build -o $(BUILD_OUT) cmd/generate-config/main.go cmd/generate-config/config_template.go
	@./$(BUILD_OUT) cmd/generate-config/system_vars_def.toml
	@mv -f cmd/generate-config/system_vars_config.toml .
	@mv -f cmd/generate-config/system_vars.go pkg/config
	@mv -f cmd/generate-config/system_vars_test.go pkg/config

# Building mo-server binary
.PHONY: build
build: cmd/db-server/main.go
	$(info >>> Building mo-server)
	@go build -o $(BIN_NAME) cmd/db-server/main.go

# Building mo-server binary for debugging, it uses the latest MatrixCube from master.
.PHONY: debug
debug: cmd/db-server/main.go
	go get github.com/matrixorigin/matrixcube
	go mod tidy
	go build -tags debug -o $(BIN_NAME) cmd/db-server/main.go

# Excluding frontend test cases temporarily
# Argument SKIP_TEST to skip a specific go test
.PHONY: test
test:
	$(info >>> Running vet and UT)
	@cd optools && ./run_unit_test.sh $(VET_REPORT) $(UT_REPORT) $(SKIP_TEST)

# Running build verification tests
.PHONY: bvt
bvt: mo-server
	$(info >>> Running BVT)
	@cd optools && ./run_bvt.sh

# Tear down
.PHONY: clean
AOE_TEST_TMP = pkg/vm/engine/aoe/test/test
clean:
	$(info >>> Cleaning up)
	@go clean -testcache
	@TMP_FILES=($(BUILD_OUT) $(BIN_ID)); \
	for file in $${TMP_FILES[@]}; do \
		if [ -f "$$file" ]; then echo "Deleting file $$file"; rm "$$file"; fi \
	done
	@TMP_DIRS=($(BUILD_LOGS) $(AOE_TEST_TMP)); \
	for dir in $${TMP_DIRS[@]}; do \
		if [ -d "$$dir" ]; then echo "Deleting directory $$dir"; rm -rf ./"$$dir"; fi \
	done