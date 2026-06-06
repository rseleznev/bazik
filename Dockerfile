FROM golang:1.25-trixie AS builder
WORKDIR /bazik
COPY . .
RUN make

FROM ubuntu:22.04
COPY --from=builder ./bazik .
ENTRYPOINT ["./bazik"]