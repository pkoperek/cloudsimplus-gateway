release:
	./gradlew dockerBuildImage
	docker tag pkoperek/cloudsimplus-gateway:0.5 pkoperek/cloudsimplus-gateway:latest	
	docker push pkoperek/cloudsimplus-gateway:latest
