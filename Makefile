APP_EXECUTABLE=bsky-firehose

tidy: ## runs tidy to fix go.mod dependencies
	go mod tidy

vet: ## Run go vet to catch potential bugs
	@echo "Running go vet..."
	go vet ./...

fmt: ## Format Go source code
	@echo "Formatting code..."
	gofmt -w .

test: ## Run all Go tests
	@echo "Running tests..."
	go test ./...

build: 
	mkdir -p out/
	@echo "Building $(APP_NAME)..."
	go build -o ./out/$(APP_EXECUTABLE) ./cmd/$(APP_EXECUTABLE)
	@echo "Build passed"

run: 
	make build
	chmod +x out/$(APP_EXECUTABLE)
	./out/$(APP_EXECUTABLE)

clean: ## cleans binary and other generated files
	go clean
	rm -rf out/

.PHONY: all build run vet tidy clean