VITASTOR_VERSION ?= v1.9.1

all: build push

build:
	@docker build --rm -t vitalif/vitastor-csi:$(VITASTOR_VERSION) .

push:
	@docker push vitalif/vitastor-csi:$(VITASTOR_VERSION)
