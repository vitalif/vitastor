VERSION ?= v0.8.2

all: build push

build:
	@docker build --rm -t vitalif/vitastor-csi:$(VERSION) .

push:
	@docker push vitalif/vitastor-csi:$(VERSION)
