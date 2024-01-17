VERSION ?= v1.4.1

all: build push

build:
	@docker build --rm -t vitalif/vitastor-csi:$(VERSION) .

push:
	@docker push vitalif/vitastor-csi:$(VERSION)
