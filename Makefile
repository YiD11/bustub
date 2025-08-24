.PHONY: clean

clean:
	rm -r ./build

build:
	mkdir -p ./build
	cd ./build && cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_POLICY_VERSION_MINIMUM=3.5 .. && make -j8
	cd ..