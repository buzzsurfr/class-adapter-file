FROM golang:1.15.5 AS builder
WORKDIR /go/
RUN go get -u github.com/virtual-class-tutor/class-adapter-file

FROM scratch
COPY --from=builder /go/bin/