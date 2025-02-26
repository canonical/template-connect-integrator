BUILD_DIRECTORY=./build
IMPL=integration
NAME=template-connect-integrator
DATA_INTERFACE=mysql_client

clean:
	rm -rf $(BUILD_DIRECTORY)

lint:
	tox -e lint

build: clean lint
	mkdir -p $(BUILD_DIRECTORY)

	BUILD_DIRECTORY=$(BUILD_DIRECTORY) IMPL=$(IMPL) NAME=$(NAME) DATA_INTERFACE=$(DATA_INTERFACE) tox -e render

	cd $(BUILD_DIRECTORY) && charmcraft pack

deploy: build
	juju deploy $(BUILD_DIRECTORY)/bundle.zip

release: build
	charmcraft upload $(BUILD_DIRECTORY)/*.zip --name kafka-bundle --release=$(TRACK)/edge
