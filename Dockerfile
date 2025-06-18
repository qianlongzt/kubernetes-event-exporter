FROM --platform=$BUILDPLATFORM golang:1.24 AS builder

ARG TARGETOS TARGETARCH

ARG VERSION
ENV PKG=github.com/resmoio/kubernetes-event-exporter/pkg

ADD . /app
WORKDIR /app
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 GO11MODULE=on go build -trimpath -ldflags="-s -w -X ${PKG}/version.Version=${VERSION}" -a -o /main .

FROM gcr.io/distroless/static:nonroot
COPY --from=builder --chown=nonroot:nonroot /main /kubernetes-event-exporter

# https://github.com/GoogleContainerTools/distroless/blob/main/base/base.bzl#L8C1-L9C1
USER 65532

ENTRYPOINT ["/kubernetes-event-exporter"]
