VERSION ?= v0.8.6

all: build push

build:
	@docker build --rm -t vitalif/vitastor-csi:$(VERSION) .

push:
	@docker push vitalif/vitastor-csi:$(VERSION)
