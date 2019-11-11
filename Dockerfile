FROM golang:1.13-alpine as builder
WORKDIR /gcsproxy
COPY . .
RUN CGO_ENABLED=0 go install

FROM alpine
COPY --from=builder /go/bin/gcsproxy /gcsproxy
CMD ["/gcsproxy"]
