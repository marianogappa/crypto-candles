format:
	go mod verify
	go build -v ./...
	go vet ./...
	staticcheck ./...
	golint ./...

test: ## Run tests
test:
	go test -race -vet=off ./...

push: test format
	@git push

help: ## prints this help
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help

.PHONY: format test help push
