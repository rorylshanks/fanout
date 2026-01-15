ARG GO_VERSION=1.24

FROM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/fanout ./cmd/fanout

FROM ubuntu:latest
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && apt install -y ca-certificates
COPY --from=build /out/fanout /usr/local/bin/fanout
ENTRYPOINT ["/usr/local/bin/fanout"]
