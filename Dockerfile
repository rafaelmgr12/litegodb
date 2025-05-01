FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod tidy

COPY . .

RUN go build -o litegodb ./cmd/server/main.go

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/litegodb .
COPY config.yaml .

VOLUME /app/data

EXPOSE 8080

CMD ["./litegodb", "--config", "config.yaml"]
