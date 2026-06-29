# Build stage
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY log_pipeline/go.mod log_pipeline/go.sum ./
RUN go mod download
COPY log_pipeline/ .
RUN CGO_ENABLED=0 GOOS=linux go build -o log_pipeline .

# Run stage
FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app/
COPY --from=builder /app/log_pipeline .
CMD ["./log_pipeline"]
