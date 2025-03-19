BUILD_DIRECTORY=./build
IMPL=integration
NAME=template-connect-integrator
DATA_INTERFACE=mysql_client
SUBSTRATE=vm

clean:
	rm -rf $(BUILD_DIRECTORY)

lint:
	tox -e lint

build: clean lint
	mkdir -p $(BUILD_DIRECTORY)

	BUILD_DIRECTORY=$(BUILD_DIRECTORY) IMPL=$(IMPL) NAME=$(NAME) DATA_INTERFACE=$(DATA_INTERFACE) SUBSTRATE=$(SUBSTRATE) tox -e render

deploy: build
	juju deploy $(BUILD_DIRECTORY)/bundle.zip

release: build
	charmcraft upload $(BUILD_DIRECTORY)/*.zip --name kafka-bundle --release=$(TRACK)/edge
