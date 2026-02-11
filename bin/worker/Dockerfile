FROM python:3.8-slim-bullseye

ENV HOME="/root/"

# Base tools and build deps (including jq and Go for ProjectDiscovery tools)
RUN apt-get update \
    && apt-get install -yq apt-utils build-essential curl gcc wget \
    readline-common neovim git zsh zsh-syntax-highlighting zsh-autosuggestions jq python3-pip unzip p7zip libpcap-dev rubygems ruby-dev grc ca-certificates golang-go \
    && rm -rf /var/lib/apt/lists/*

ENV GOPATH=/root/go
ENV PATH="$GOPATH/bin:${PATH}"

# Install ProjectDiscovery tools via Go (official method)
RUN set -eux; \
    go install -v github.com/projectdiscovery/subfinder/v2/cmd/subfinder@latest; \
    go install -v github.com/projectdiscovery/httpx/cmd/httpx@latest; \
    go install -v github.com/projectdiscovery/dnsx/cmd/dnsx@latest; \
    subfinder -version || subfinder -h; \
    httpx -version || httpx -h; \
    dnsx -version || dnsx -h

# Application code: use this repo's worker code, no Axiom
WORKDIR /app
COPY . /app/bin/worker
RUN pip3 install -r /app/bin/worker/requirements.txt

ENTRYPOINT ["python3", "/app/bin/worker/worker.py"]
