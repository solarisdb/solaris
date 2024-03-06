FROM golang:1.22.1 as builder

WORKDIR /usr/src/solaris

COPY . .

RUN apt update && apt -y --no-install-recommends install openssh-client git && \
        mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts && \
        git config --global url."git@github.com:".insteadOf "https://github.com"

RUN go env -w GOPRIVATE="github.com/solarisdb/*"

RUN --mount=type=ssh CGO_ENABLED=0 make all

FROM alpine:3.16

ADD https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.12/grpc_health_probe-linux-amd64 /bin/grpc_health_probe

RUN chmod +x /bin/grpc_health_probe

EXPOSE 50051

WORKDIR /app

COPY --from=builder /usr/src/solaris/build/solaris .
COPY --from=builder /usr/src/solaris/config/solaris.yaml .

CMD ["/app/solaris", "start", "--config", "/app/solaris.yaml"]
