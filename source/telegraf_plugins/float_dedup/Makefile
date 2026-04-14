GO      ?= /usr/local/go/bin/go
BINARY  := float_dedup

.PHONY: all clean tidy

all: $(BINARY)

$(BINARY): main.go go.mod go.sum
	$(GO) build -o $(BINARY) .

go.sum: go.mod
	$(GO) mod tidy

tidy:
	$(GO) mod tidy

clean:
	rm -f $(BINARY)
