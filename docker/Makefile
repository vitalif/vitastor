VITASTOR_VERSION ?= v1.11.0

all: build push

build:
	@docker build --rm -t vitalif/vitastor:$(VITASTOR_VERSION) .

push:
	@docker push vitalif/vitastor:$(VITASTOR_VERSION)
