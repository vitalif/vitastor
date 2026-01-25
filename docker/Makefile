VITASTOR_VERSION ?= v3.0.2

all: build push

build:
	@docker build --no-cache --rm -t vitalif/vitastor:$(VITASTOR_VERSION) .

push:
	@docker push vitalif/vitastor:$(VITASTOR_VERSION)
