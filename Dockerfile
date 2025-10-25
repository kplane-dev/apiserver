# syntax=docker/dockerfile:1
FROM gcr.io/distroless/static:nonroot

ARG TARGETARCH
ARG BIN=./.dev/bin/apiserver-linux-${TARGETARCH}

COPY ${BIN} /apiserver

USER 65532:65532
ENTRYPOINT ["/apiserver"]

