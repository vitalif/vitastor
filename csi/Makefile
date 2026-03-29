VITASTOR_VERSION ?= v3.0.8

all: build push

build:
	@docker build --rm -t vitalif/vitastor-csi:$(VITASTOR_VERSION) .

push:
	@docker push vitalif/vitastor-csi:$(VITASTOR_VERSION)
