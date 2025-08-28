
FROM golang:alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o kv-server ./cmd/server.go

FROM alpine:3.18
WORKDIR /app
COPY --from=build /app/kv-server /usr/local/bin/kv-server
ENTRYPOINT ["kv-server"]
