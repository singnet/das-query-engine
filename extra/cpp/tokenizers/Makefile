clean:
	@rm -rf ./build

build-tests: clean
	@mkdir -p ./build \
		&& cmake -S . -B ./build \
		&& cmake --build ./build --parallel $(nproc)

unit-tests: build-tests
	make -C ./build test
