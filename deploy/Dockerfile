# Build context is the repository root:
#   docker build -f deploy/Dockerfile -t traderbot .
# (docker compose -f deploy/docker-compose.yml does this for you.)

FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Static binary: distroless/static has no libc. Time zones are embedded via
# time/tzdata (internal/config), CA certificates come with the base image.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/traderbot ./cmd/traderbot

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/traderbot /traderbot
WORKDIR /app
USER nonroot:nonroot
EXPOSE 8080
# The image has no shell or curl; the binary probes itself.
HEALTHCHECK --interval=30s --timeout=5s --start-period=120s --retries=3 \
  CMD ["/traderbot", "-healthcheck", "-http", ":8080"]
ENTRYPOINT ["/traderbot"]
CMD ["-config", "/app/configs/config.yaml", "-http", ":8080"]
