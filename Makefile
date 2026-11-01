IMG ?= emr-zombie-killer:dev
CONTROLLER_GEN ?= $(shell go env GOPATH)/bin/controller-gen

.PHONY: generate test build docker-build deploy undeploy
generate: ## regenerate deepcopy + CRD
	$(CONTROLLER_GEN) object paths=./api/...
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:artifacts:config=config/crd

test:
	go test -race ./...

build:
	go build -o bin/manager ./cmd/manager

docker-build:
	docker build --platform linux/amd64 -t $(IMG) .

deploy:
	kubectl apply -f config/crd -f config/rbac -f config/manager

undeploy:
	kubectl delete -f config/manager -f config/rbac -f config/crd
