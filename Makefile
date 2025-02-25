BUILD_DIRECTORY=./build
IMPL=integration

clean:
	rm -rf $(BUILD_DIRECTORY)

lint:
	tox -e lint

build: clean lint
	mkdir -p $(BUILD_DIRECTORY)

	BUILD_DIRECTORY=$(BUILD_DIRECTORY) IMPL=$(IMPL) tox -e render

	cd $(BUILD_DIRECTORY) && charmcraft pack --destructive-mode

deploy: build
	juju deploy $(BUILD_DIRECTORY)/bundle.zip

release: build
	charmcraft upload $(BUILD_DIRECTORY)/*.zip --name kafka-bundle --release=$(TRACK)/edge
