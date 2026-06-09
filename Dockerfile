FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /raft-cluster ./cmd/raft-cluster

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /raft-cluster /raft-cluster
CMD ["/raft-cluster"]
