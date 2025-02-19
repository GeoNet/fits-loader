FROM ghcr.io/geonet/base-images/golang:1.23-alpine3.21 as build
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /go/src/github.com/GeoNet/fits-loader
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags "-X main.Prefix=${i}/${VERSION} -extldflags -static" -o fits-loader .

FROM ghcr.io/geonet/base-images/python:3.11.4-alpine3.18
RUN apk add --no-cache bash jq
RUN pip3 install awscli --upgrade
WORKDIR /home
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=build /go/src/github.com/GeoNet/fits-loader/fits-loader .
COPY ./fits-loader-docker/init-sqs-process.sh .
COPY ./fits-loader-docker/sqs-process.sh .
CMD ["./init-sqs-process.sh"]

