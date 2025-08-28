

FROM golang:alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o kv-server ./cmd/server/main.go
# Optional: build client binary
# RUN go build -o kv-client ./cmd/client/main.go

FROM alpine:3.18
WORKDIR /app
COPY --from=build /app/kv-server /usr/local/bin/kv-server
# Optional: copy client binary
# COPY --from=build /app/kv-client /usr/local/bin/kv-client
ENTRYPOINT ["kv-server"]
