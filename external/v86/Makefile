.PHONY: build clobber

build:
	docker build $(if $(wildcard libv86.js),,--no-cache) --load -t wanix-v86-build .
	docker run --rm -v $(PWD):/dst wanix-v86-build
	{ echo "window.process = undefined;"; cat libv86.js; } > libv86.js.tmp && mv libv86.js.tmp libv86.js

clobber:
	rm -f libv86.js
	rm -f v86.wasm
	rm -f seabios.bin
	rm -f vgabios.bin