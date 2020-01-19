.PHONY: gen
gen:
	go generate ./types/

.PHONY: test
test:
	go test ./
