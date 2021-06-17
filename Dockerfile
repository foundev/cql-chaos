##
## Build
##

FROM golang:1.16-buster AS build

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY *.go .

RUN go build -o /cqlchaos


##
## Deploy
##
FROM debian:buster-slim
COPY --from=build /cqlchaos /cqlchaos
ENTRYPOINT ["cqlchaos", "-h"]
