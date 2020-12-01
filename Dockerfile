ARG version 

FROM golang:1.14

ENV GO111MODULE on

WORKDIR /usr/src/app

COPY podips-reader.go /usr/src/app
COPY go.mod /usr/src/app

RUN go mod download

RUN go build podips-reader.go

USER nobody 

ENTRYPOINT ["/usr/local/bin/dumb-init"]
CMD [ "podips-reader" ]
