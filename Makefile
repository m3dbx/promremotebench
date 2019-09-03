golangci-lint_version      := v1.17.1
gopath_prefix              := $(HOME)/src
promremote_package         := github.com/m3db/promremotebench# change this to m3dbx
promremote_package_path    := $(gopath_prefix)/$(promremote_package)/src/cmd/promremotebench
src_relative_path          := ./src/

install-linter:
	echo "Installing golangci-lint..."
	@PATH=$(GOPATH)/bin:$(PATH) which golangci-lint > /dev/null || "(curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(go env GOPATH)/bin golangci-lint_version"
	@PATH=$(GOPATH)/bin:$(PATH) which golangci-lint > /dev/null || (echo "golangci-lint install failed" && exit 1)

linter:
	make install-linter
	@echo "--- linting promremotebench in $(promremote_package_path)"
	cd $(src_relative_path) && golangci-lint run 
