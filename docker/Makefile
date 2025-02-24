VITASTOR_VERSION ?= v1.10.1

all: build push

build:
	@docker build --rm -t vitalif/vitastor:$(VITASTOR_VERSION) .

push:
	@docker push vitalif/vitastor:$(VITASTOR_VERSION)
