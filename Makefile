release:
	./gradlew dockerBuildImage
	docker tag pkoperek/cloudsimplus-gateway:0.2 pkoperek/cloudsimplus-gateway:latest	
	docker push pkoperek/cloudsimplus-gateway:latest