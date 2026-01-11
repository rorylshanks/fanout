BIN_DIR := bin
BINARY := fanout
CMD_DIR := ./cmd/fanout

.PHONY: build
build:
	@mkdir -p $(BIN_DIR)
	GOOS=linux GOARCH=amd64 go build -o $(BIN_DIR)/$(BINARY)-linux-amd64 $(CMD_DIR)
	GOOS=linux GOARCH=arm64 go build -o $(BIN_DIR)/$(BINARY)-linux-arm64 $(CMD_DIR)
	GOOS=darwin GOARCH=amd64 go build -o $(BIN_DIR)/$(BINARY)-darwin-amd64 $(CMD_DIR)
	GOOS=darwin GOARCH=arm64 go build -o $(BIN_DIR)/$(BINARY)-darwin-arm64 $(CMD_DIR)
	GOOS=windows GOARCH=amd64 go build -o $(BIN_DIR)/$(BINARY)-windows-amd64.exe $(CMD_DIR)
	GOOS=windows GOARCH=arm64 go build -o $(BIN_DIR)/$(BINARY)-windows-arm64.exe $(CMD_DIR)
