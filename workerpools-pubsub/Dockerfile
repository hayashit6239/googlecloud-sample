FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY . .

RUN go mod tidy
RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o worker ./cmd/worker

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/worker .

RUN mkdir -p /tmp/downloads

EXPOSE 8080

CMD ["./worker"]