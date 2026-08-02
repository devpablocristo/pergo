# Stage 1: Build the Go binary. Both stages are pinned by manifest-list digest so
# a rebuild cannot silently pick up a different base image.
FROM golang:1.26.5-alpine@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS builder

WORKDIR /app

# Install git, certificates, and templ CLI
RUN apk add --no-cache git ca-certificates
RUN go install github.com/a-h/templ/cmd/templ@v0.3.1020

# Copy the filtered build context. .dockerignore excludes VCS metadata,
# operator env files, credentials, local plans, and build artifacts.
COPY . .

RUN go mod download

# Generate templ files
RUN templ generate ./...

# Build the CGO-free static binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o pergo ./cmd/pergo

# Stage 2: Minimal non-root runtime image
FROM gcr.io/distroless/static-debian12:nonroot@sha256:f5b485ea962d9bd1186b2f6b3a061191539b905b82ec395de78cbfae51f20e35

WORKDIR /app

# Copy root CA certificates
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the compiled binary
COPY --from=builder /app/pergo .

# Copy static assets (needed for admin UI)
COPY --from=builder /app/static ./static

# Expose port
EXPOSE 8080

# Keep the runtime identity explicit even if the base image defaults change.
USER 65532:65532

# Command to run the application
ENTRYPOINT ["/app/pergo"]
