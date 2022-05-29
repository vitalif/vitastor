VERSION ?= v0.7.0

all: build push

build:
	@docker build --rm -t vitalif/vitastor-csi:$(VERSION) .

push:
	@docker push vitalif/vitastor-csi:$(VERSION)
