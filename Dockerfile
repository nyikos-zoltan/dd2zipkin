FROM rust:1.84.1-alpine as builder
RUN apk add musl-dev
WORKDIR /usr/src/dd2zipkin
COPY . .

RUN cargo install --path .

FROM alpine
COPY --from=builder /usr/local/cargo/bin/dd2zipkin /usr/local/bin/dd2zipkin
CMD ["dd2zipkin"]
