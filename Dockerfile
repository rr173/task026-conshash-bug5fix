# syntax=docker/dockerfile:1
FROM docker.m.daocloud.io/library/golang:1.26.3-bookworm AS builder
WORKDIR /src
COPY . .
ENV GOTOOLCHAIN=local \
    GOPROXY=https://goproxy.cn,direct \
    GOSUMDB=sum.golang.google.cn \
    CGO_ENABLED=0
RUN go mod vendor
RUN go build -mod=vendor -o /out/conshash .

FROM docker.m.daocloud.io/library/alpine:3.20
COPY --from=builder /out/conshash /usr/local/bin/conshash
ENTRYPOINT ["/usr/local/bin/conshash"]
CMD ["--smoke-test"]
