VITASTOR_VERSION ?= v2.2.0

all: build push

build:
	@docker build --no-cache --rm -t vitalif/vitastor:$(VITASTOR_VERSION) .

push:
	@docker push vitalif/vitastor:$(VITASTOR_VERSION)
