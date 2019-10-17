FROM golang:1.13.1-alpine as build
WORKDIR /go/src/github.com/GeoNet/fits-loader
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags "-X main.Prefix=${i}/${VERSION} -extldflags -static" -o fits-loader .

FROM python:alpine3.10
RUN apk add --no-cache bash jq
RUN pip3 install awscli --upgrade
WORKDIR /home
COPY --from=build /go/src/github.com/GeoNet/fits-loader/fits-loader .
COPY ./fits-loader-docker/init-sqs-process.sh .
COPY ./fits-loader-docker/sqs-process.sh .
CMD ["./init-sqs-process.sh"]

