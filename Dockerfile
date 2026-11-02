# ==============================================================================
# STAGE 1: Build the Go binary
# ==============================================================================
FROM golang:1.27-alpine AS builder

# Install git if your private dependencies require it
RUN apk add --no-cache git

RUN mkdir -p /code
ADD ./ /code
WORKDIR /code
COPY .env /code/.env

# RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler ca-certificates git && rm -rf /var/lib/apt/lists/*
# RUN go install github.com/bufbuild/buf/cmd/buf@v1.44.0
# RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
# RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1


RUN go mod tidy -compat=1.17
# Build the application
# CGO_ENABLED=0 disables dynamic links for a fully self-contained static binary
# GOOS=linux ensures it is targeted to run in a Linux container
#
#RUN CGO_ENABLED=0 GOOS=linux go build -o mainfile main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o mainfile cmd/grpc/grpc_main.go

# ==============================================================================
# STAGE 2: Deployment runtime environment
# ==============================================================================
# Using 'alpine' keeps the image small (~10-20MB). Alternatively, use 'scratch' 
# if you want a completely empty base image (~server binary size only).
FROM alpine:3.20

RUN mkdir -p /code
WORKDIR /code

COPY --from=builder /code/.env /code/.env
COPY --from=builder /code/mainfile /code/mainfile

EXPOSE 9092 8080 8001

ENTRYPOINT  ["./mainfile"]
