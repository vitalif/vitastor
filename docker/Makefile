VITASTOR_VERSION ?= v2.0.0

all: build push

build:
	@docker build --rm -t vitalif/vitastor:$(VITASTOR_VERSION) .

push:
	@docker push vitalif/vitastor:$(VITASTOR_VERSION)
