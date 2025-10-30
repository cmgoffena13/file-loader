FROM python:3.12-slim-bookworm
ENV PYTHONBUFFERED=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        ca-certificates && \
    apt-get upgrade -y openssl

ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh

ENV PATH="/root/.local/bin/:$PATH"
ENV UV_LINK_MODE=copy

WORKDIR /fileloader

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --compile-bytecode

COPY . .

CMD ["uv", "run", "--", "python", "main.py"]
