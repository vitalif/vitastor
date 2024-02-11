VERSION ?= v1.4.4

all: build push

build:
	@docker build --rm -t vitalif/vitastor-csi:$(VERSION) .

push:
	@docker push vitalif/vitastor-csi:$(VERSION)
