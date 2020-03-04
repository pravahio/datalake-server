build:
	go build -o ./bin/datalake serve.go

build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./bin/datalake_amd64 serve.go

docker-build: build-linux
	docker build -t "pravahio/datalake:latest" .