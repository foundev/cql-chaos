.PHONY: all
all: test build image publish

.PHONY: test
test:
	go test .

.PHONY: build
build:
	go build -o ./bin/cqlchaos

.PHONY: image
image: build
	docker build -t ryansvihla/cqlchaos:latest .

.PHONY: publish
publish: image
	docker push ryansvihla/cqlchaos:latest
