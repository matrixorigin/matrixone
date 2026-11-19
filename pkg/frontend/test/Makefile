.PHONY: generate_mock generate_fe_mock

# Generate only the interfaces consumed by the frontend test helpers.
# The root target owns native build flags and the pinned mockgen version.
generate_mock:
	$(MAKE) -C ../../.. generate-frontend-mocks

generate_fe_mock: generate_mock
