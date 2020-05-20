VERSION=1.6.5
release:
	./gradlew dockerBuildImage
	docker tag pkoperek/cloudsimplus-gateway:${VERSION} pkoperek/cloudsimplus-gateway:latest
	docker push pkoperek/cloudsimplus-gateway:latest
	docker push pkoperek/cloudsimplus-gateway:${VERSION}
