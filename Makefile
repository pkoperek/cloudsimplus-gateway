IMAGE_W_TAG := $(shell grep dockerImageTag build.gradle | cut -d\' -f2)

release:
	@echo IMAGE_W_TAG  $(IMAGE_W_TAG)
	./gradlew clean
	./gradlew dockerBuildImage
	docker tag $(IMAGE_W_TAG) pkoperek/cloudsimplus-gateway:latest
	docker push pkoperek/cloudsimplus-gateway:latest
	docker push $(IMAGE_W_TAG)
