# syntax=docker/dockerfile:1.6

# Go >= 1.25.10 corrige CVEs HIGH do stdlib detectados pelo Trivy (gobinary)
FROM golang:1.26.3-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN go version

RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/donation-service .

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=builder /out/donation-service /app/donation-service

EXPOSE 8002

ENTRYPOINT ["/app/donation-service"]