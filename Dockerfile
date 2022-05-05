# stage 1: build
FROM golang:1.18-alpine AS builder
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

# Install git
RUN apk add --update git

# Add source code
RUN mkdir -p /src
ADD ./src /src

# Build binary
RUN cd /src && go build ./cmd/promremotebench

# stage 2: lightweight "release"
FROM alpine:latest
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

COPY --from=builder /src/promremotebench /bin/
COPY --from=builder /src/config/promremotebench.yml /etc/promremotebench/promremotebench.yml 

RUN apk add -U --no-cache ca-certificates

ENTRYPOINT [ "/bin/promremotebench" ]
CMD [ "-config", "/etc/promremotebench/promremotebench.yml" ]
