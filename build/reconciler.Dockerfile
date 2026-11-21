# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=arm64
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/reconciler ./cmd/reconciler

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/reconciler /reconciler
USER nonroot:nonroot
ENTRYPOINT ["/reconciler"]
