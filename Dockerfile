# Build a static binary, then ship it alone in a minimal image without a shell.
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/panoptes ./cmd/panoptes

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/panoptes /panoptes
EXPOSE 8080
ENTRYPOINT ["/panoptes"]
