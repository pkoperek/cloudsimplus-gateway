release:
	./gradlew dockerBuildImage
	docker push pkoperek/cloudsimplus-gateway:ppam19
