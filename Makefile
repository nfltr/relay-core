# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

# A go.work above the checkout never joins this module's build.
export GOWORK := off

GOVULNCHECK_VERSION := v1.8.0

.PHONY: check build vet test test-race fuzz-short fuzz-long check-gosec check-semgrep check-govulncheck check-publish check-binary-size examples readme-quickstart
check: build vet test-race fuzz-short check-gosec check-semgrep check-govulncheck check-publish check-binary-size examples readme-quickstart

build:
	CGO_ENABLED=0 go build ./...

vet:
	go vet ./...

test:
	go test -count=1 -timeout 30m ./...

test-race:
	go test -count=1 -race -timeout 30m ./...

# FUZZTIME per target (default 10s); FUZZPARALLEL caps the workers.
fuzz-short:
	./scripts/fuzz-short.sh

# Every target for FUZZTIME (default 30m) each.
fuzz-long:
	FUZZTIME=$(or $(FUZZTIME),30m) ./scripts/fuzz-short.sh

# Pinned SAST gated by scripts/{gosec,semgrep}-baseline.txt (semgrep needs
# docker; without it the check says so and passes).
check-gosec:
	./scripts/check-gosec.sh

check-semgrep:
	./scripts/check-semgrep.sh

# Needs network (vuln.go.dev).
check-govulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

# Nothing unpublishable in the repository's files: email addresses, public
# IPv4 addresses, internal host names, home directory paths
# (scripts/check-publish.sh; justified lines in scripts/publish-allow.txt).
check-publish:
	./scripts/check-publish.sh

# The stripped linux/amd64 nfltr-relay-core's size (scripts/check-binary-size.sh).
# Budget: measured + 5 % (2026-10-28: 20,521,120 bytes, the core linking the
# frontends it attaches: connect, http, sni); lowered when a cut shrinks it,
# raised only with the reason in CHANGELOG.md.
CORE_SIZE_BUDGET_BYTES ?= 21548000
check-binary-size:
	CORE_SIZE_BUDGET_BYTES=$(CORE_SIZE_BUDGET_BYTES) ./scripts/check-binary-size.sh

# Every program under examples/ built and run to exit 0, each under a
# timeout (scripts/run-examples.sh).
examples:
	./scripts/run-examples.sh

# The README's quickstart block, extracted and run as a reader would
# (scripts/readme-quickstart.sh).
readme-quickstart:
	./scripts/readme-quickstart.sh

# The exported API against the latest release (apidiff, pinned in the
# script): the changes must be the ones api/ lists; before the first tag it
# passes with a message. A change on purpose: CHANGELOG.md, then
# UPDATE=1 make check-api.
.PHONY: check-api
check-api:
	./scripts/check-api.sh

CI_WORKFLOW_FILE := .github/workflows/check.yml
# ci: what the GitHub workflow runs on a push (CI_WORKFLOW_FILE), run here.
# GitHub Actions is not a gate; this is, before a push (the pre-push hook)
# and after it (nfltr's Linux CI runner). scripts/ci.sh reads the steps from
# the workflow file itself, so the two cannot drift; a pass on a clean tree
# is stamped and not rerun for the same tree.
# hooks: install the pre-push hook (scripts/pre-push): `make ci` on the
# exact commit pushed to main or a tag; bypass only with
# NFLTR_CI_BYPASS=<reason> (logged to <git-common-dir>/ci-bypass.log).
.PHONY: ci hooks
ci:
	@CI_WORKFLOW=$(CI_WORKFLOW_FILE) ./scripts/ci.sh
hooks:
	@h="$$(git rev-parse --git-path hooks)/pre-push"; \
	if [ -e "$$h" ] && ! grep -q 'scripts/pre-push' "$$h"; then echo "hooks: $$h exists and is not this one; move it away first" >&2; exit 1; fi; \
	printf '#!/bin/sh\n# make hooks: scripts/pre-push of the checkout pushing (else of %s)\ns="$$(git rev-parse --show-toplevel)/scripts/pre-push"\n[ -x "$$s" ] || s=%s/scripts/pre-push\nexec "$$s" "$$@"\n' "$(CURDIR)" "$(CURDIR)" >"$$h" && chmod +x "$$h" && echo "hooks: installed $$h"
