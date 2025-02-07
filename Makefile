IMAGE=us-central1-docker.pkg.dev/bitrise-website-staging/dd2zipkin/dd2zipkin:latest
deploy:
	docker build -t ${IMAGE} .
	docker push ${IMAGE}
