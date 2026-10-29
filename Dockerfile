FROM golang:1.25-bookworm AS build
ARG SERVICE
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -trimpath -o /app ./cmd/${SERVICE}
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app /app/service
COPY migrations ./migrations
USER 65532:65532
ENTRYPOINT ["/app/service"]
