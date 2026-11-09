# Build static binaries, then ship each alone in a minimal image without a shell.
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/panoptes ./cmd/demo

# The demo scenario for `make demo`; built only when asked for.
FROM gcr.io/distroless/static-debian12:nonroot AS demo
COPY --from=build /out/demo /demo
ENTRYPOINT ["/demo"]

# The service. The last stage is the default target.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/panoptes /panoptes
EXPOSE 8080
ENTRYPOINT ["/panoptes"]
