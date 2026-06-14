# murmur cloud deploy. Provision with Terraform, push the static binary, drive
# the cluster over SSH. Learning cluster only — see README for the caveats.
#
# Prereqs: terraform, jq, ssh, an hcloud token, your key in ~/.ssh.
#   export TF_VAR_hcloud_token=...   export TF_VAR_admin_cidr=$(curl -s ifconfig.me)/32
SHELL := /usr/bin/env bash
TF    := terraform -chdir=terraform
REPO  := $(abspath ..)
DIST  := $(REPO)/dist
SSHOPTS := -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null
SSH   := ssh $(SSHOPTS) root@

.PHONY: build init plan apply deploy ps nodes run rm partition heal ssh destroy fmt

## Cross-compile the static linux binaries.
build:
	cd $(REPO) && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o $(DIST)/murmurd  ./cmd/murmurd
	cd $(REPO) && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o $(DIST)/murmurctl ./cmd/murmurctl
	@echo "built -> $(DIST)"

init:  ; $(TF) init
plan:  ; $(TF) plan
fmt:   ; $(TF) fmt
apply: ; $(TF) apply -auto-approve
	@echo "provisioned. give cloud-init ~60s, then: make deploy"

## scp the binary to every peer and (re)start the service.
deploy: build
	@set -euo pipefail; \
	for node in $$($(TF) output -json peers | jq -r 'keys[]'); do \
	  ip=$$($(TF) output -json peers | jq -r ".\"$$node\".public_ip"); \
	  echo "==> $$node ($$ip)"; \
	  scp $(SSHOPTS) $(DIST)/murmurd $(DIST)/murmurctl root@$$ip:/usr/local/bin/; \
	  $(SSH)$$ip 'systemctl restart murmurd && systemctl --no-pager -l status murmurd | head -5'; \
	done

## Cluster ops — run murmurctl ON the seed peer (any peer would do).
SEED = $$($(TF) output -raw seed_public_ip)
ps:    ; $(SSH)$(SEED) 'murmurctl --peer host-a ps'
nodes: ; $(SSH)$(SEED) 'murmurctl --peer host-a nodes'
## make run NAME=counter
run:   ; $(SSH)$(SEED) "murmurctl --peer host-a run --name $(NAME)"
## make rm NAME=counter
rm:    ; $(SSH)$(SEED) "murmurctl --peer host-a rm --name $(NAME)"

## Fault injection (real-host equivalent of sim.Partition). make partition NODE=host-a
partition: ; ./scripts/partition.sh isolate $(NODE)
heal:      ; ./scripts/partition.sh heal

## make ssh NODE=host-b
ssh: ; $(SSH)$$($(TF) output -json peers | jq -r ".\"$(NODE)\".public_ip")

## Tear it all down (stops the meter).
destroy: ; $(TF) destroy -auto-approve
