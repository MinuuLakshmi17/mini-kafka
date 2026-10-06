FROM golang:1.23 AS build
WORKDIR /src
COPY . .
RUN go build -o /out/broker ./cmd/broker && go build -o /out/mkctl ./cmd/mkctl
FROM debian:bookworm-slim
COPY --from=build /out/broker /usr/local/bin/broker
COPY --from=build /out/mkctl /usr/local/bin/mkctl
VOLUME /data
EXPOSE 9092
ENTRYPOINT ["broker","--addr",":9092","--data","/data"]
