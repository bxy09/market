.PHONY: build push
IMAGE = mviewer
TAG = 0.2

build:
	golinux
	docker build -t r.gpxtrade.com/gpx_dev/$(IMAGE):$(TAG) .
push:
	docker push r.gpxtrade.com/gpx_dev/$(IMAGE):$(TAG)
