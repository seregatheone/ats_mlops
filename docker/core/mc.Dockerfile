FROM python:3.11.17-slim-bookworm@sha256:f06d01d4ba3f8ad5af45a3ec87df19c1d317d8e8961edc0fabda61811af6f7cf AS source
ADD --checksum=sha256:95cd293c7119f16921a6dc515a1fb74a2227f19fd994b9c8b770a154e802ac44 https://codeload.github.com/minio/mc/tar.gz/7394ce0dd2a80935aded936b09fa12cbb3cb8096 /tmp/mc.tar.gz
RUN mkdir /src && tar -xzf /tmp/mc.tar.gz --strip-components=1 -C /src

FROM golang:1.26.5-alpine3.24@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS build
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOMAXPROCS=2
WORKDIR /src
COPY --from=source /src/ ./
RUN go build -p 2 -buildvcs=false -trimpath -ldflags="-s -w" -o /out/mc .

FROM python:3.11.17-slim-bookworm@sha256:f06d01d4ba3f8ad5af45a3ec87df19c1d317d8e8961edc0fabda61811af6f7cf
LABEL org.opencontainers.image.source="https://github.com/minio/mc" \
      org.opencontainers.image.revision="7394ce0dd2a80935aded936b09fa12cbb3cb8096" \
      org.opencontainers.image.licenses="AGPL-3.0"
COPY --from=build /out/mc /usr/local/bin/mc
COPY --from=source /src/LICENSE /usr/share/licenses/mc/LICENSE
COPY --chmod=0644 docker/core/minio-init.sh /opt/core/minio-init.sh
COPY configs/storage/policies/ /opt/core/policies/
USER 10001:10001
ENTRYPOINT ["/bin/sh", "/opt/core/minio-init.sh"]
