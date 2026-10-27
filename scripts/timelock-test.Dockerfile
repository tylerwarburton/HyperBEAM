# Test image for `timelock@1.0': this tree (src.tar from scripts/timelock-test.sh),
# compiled with every native library, ready for `rebar3 device test'.
FROM erlang:27
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential cmake git pkg-config libssl-dev libncurses-dev \
      ca-certificates curl unzip \
    && rm -rf /var/lib/apt/lists/*
ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl -fsSL https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable
COPY src.tar /src.tar
RUN mkdir /app && tar -xf /src.tar -C /app && rm /src.tar
WORKDIR /app
# The Makefile's buildinfo needs a git HEAD; the archive has none.
RUN git config --global user.email t@t && git config --global user.name t \
    && git config --global --add safe.directory /app \
    && git init -q && git add -A && git commit -qm timelock-test
RUN rebar3 compile && test -f priv/blst_nif.so
