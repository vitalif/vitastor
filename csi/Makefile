VITASTOR_VERSION ?= v2.2.2

all: build push

build:
	@docker build --rm -t vitalif/vitastor-csi:$(VITASTOR_VERSION) .

push:
	@docker push vitalif/vitastor-csi:$(VITASTOR_VERSION)
