# syntax=docker/dockerfile:1
FROM golang:1.20-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o kv-g

FROM alpine:3.18
WORKDIR /app
COPY --from=build /app/kv-g /usr/local/bin/kv-g
ENTRYPOINT ["kv-g"]
CMD ["--port", "8081", "--id", "node1"]
