FROM openjdk:8

ENV RIEMANN_VERSION 0.3.2

RUN curl -skL https://github.com/riemann/riemann/releases/download/${RIEMANN_VERSION}/riemann-${RIEMANN_VERSION}.tar.bz2 \
    | bunzip2 -c - \
    | tar xf - \
    && \
    mv /riemann-${RIEMANN_VERSION} /riemann

WORKDIR /riemann

EXPOSE 5555/tcp 5555/udp 5556

CMD ["bin/riemann", "etc/riemann.config"]
