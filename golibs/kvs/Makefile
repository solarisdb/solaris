GENPROTO_DIR=./genproto

# Help by default
default: help

.PHONY: clean
clean:
	@rm -rf ${GENPROTO_DIR}/*

.PHONY: protoc
protoc:
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

.PHONY: generate
generate: ## generating gRPC files
	@echo "generating proto files"
	@protoc --go_out=${GENPROTO_DIR} proto/golibskvs/v1/record.proto

.PHONY: redis-start
redis-start: ## start redis server in docker
	@docker run -d --name redis-stack -p 6379:6379 -p 8001:8001 redis/redis-stack:latest

.PHONY: redis-stop
redis-stop: ## stop redis
	@docker stop redis-stack

.PHONY: test
test: ## run tests
	@go test -v -cover ./...

# generate help info from comments: thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help: ## help information about make commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
