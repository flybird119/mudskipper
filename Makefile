VERSION=1.0.0
BUILD=$(shell git rev-list --count HEAD)

mudskipper-dpkg:
	go build
	go install
	mkdir -p deb/mudskipper/usr/local/bin
	mkdir -p deb/mudskipper/etc/init.d
	cp $(GOPATH)/bin/mudskipper  deb/mudskipper/usr/local/bin
	cp mudskipper_ctl deb/mudskipper/etc/init.d
	cp mudskipper.conf deb/mudskipper/etc/mudskipper.conf
	fpm -s dir -t deb -n mudskipper -v $(VERSION)-$(BUILD) -C deb/mudskipper .

.PHONY : clean
clean:
	rm -rf deb
	rm -rf mudskipper
	rm -rf *.deb
