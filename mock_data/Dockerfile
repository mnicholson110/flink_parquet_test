FROM golang:latest as builder
WORKDIR /build
COPY ./app/ ./
RUN CGO_ENABLED=0 go build -o ./main

FROM scratch
COPY --from=builder /build/main ./main
CMD ["./main"]
