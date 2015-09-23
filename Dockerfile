FROM golang:onbuild
EXPOSE 8080
ENTRYPOINT ["/go/bin/app"]
