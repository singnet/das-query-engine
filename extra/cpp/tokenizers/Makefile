clean:
	@rm -rf ./build

build-tests: clean
	@mkdir -p ./build \
		&& cmake -S . -B ./build \
		&& cmake --build ./build

unit-tests: build-tests
	make -C ./build test
