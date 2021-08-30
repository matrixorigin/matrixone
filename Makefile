# This Makefile is to build MatrixOne

BIN_ID = mo-server
BUILD_OUT = gen_config
UT_REPORT = ut_reports.txt
VET_REPORT = vet_reports.txt

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
	@go build -o $(BIN_ID) cmd/db-server/main.go

# Set test timeout to 15 minutes
# Excluding frontend test cases temporarily
.PHONY: test
test:
	$(info >>> Running vet and UT)
	@cd optools && ./run_unit_test.sh $(VET_REPORT) $(UT_REPORT)

# Running build verification tests
.PHONY: bvt
bvt: mo-server
	$(info >>> Running BVT)
	@cd optools && ./run_bvt.sh

# Tear down
.PHONY: clean
clean:
	$(info >>> Cleaning up)
	@go clean -testcache
	@TMP_FILES=($(BUILD_OUT) $(BIN_ID) $(UT_REPORT) $(VET_REPORT)); \
	for file in $${TMP_FILES[@]}; do \
		if [ -f "$$file" ]; then echo "Removing $$file"; rm "$$file"; fi \
	done