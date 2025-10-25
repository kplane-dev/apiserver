# syntax=docker/dockerfile:1
FROM gcr.io/distroless/static:nonroot

ARG TARGETARCH
# Copy the prebuilt binary for the target arch from the build context
COPY ./.dev/bin/apiserver-linux-${TARGETARCH} /apiserver

USER 65532:65532
ENTRYPOINT ["/apiserver"]

