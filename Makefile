.PHONY: gen
gen:
	go generate ./types/

.PHONY: test
test:
	go test ./


.PHONY: protoc
protoc:
	go get -mod=readonly github.com/gogo/protobuf/protoc-gen-gogofaster
