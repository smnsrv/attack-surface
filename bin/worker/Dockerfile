FROM python:3.8-slim-bullseye

ENV HOME="/root/"

# Base tools and build deps (including jq as requested)
RUN apt-get update \
    && apt-get install -yq apt-utils build-essential curl gcc wget \
    readline-common neovim git zsh zsh-syntax-highlighting zsh-autosuggestions jq python3-pip unzip p7zip libpcap-dev rubygems ruby-dev grc ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install ProjectDiscovery tools: subfinder, httpx, dnsx (via official install scripts)
RUN set -eux; \
    cd /tmp; \
    curl -sSfL https://raw.githubusercontent.com/projectdiscovery/subfinder/main/install.sh | sh -s -- -b /usr/local/bin; \
    subfinder -version || subfinder -h

RUN set -eux; \
    cd /tmp; \
    curl -sSfL https://raw.githubusercontent.com/projectdiscovery/httpx/main/install.sh | sh -s -- -b /usr/local/bin; \
    httpx -version || httpx -h

RUN set -eux; \
    cd /tmp; \
    curl -sSfL https://raw.githubusercontent.com/projectdiscovery/dnsx/main/install.sh | sh -s -- -b /usr/local/bin; \
    dnsx -version || dnsx -h

# Application code: use this repo's worker code, no Axiom
WORKDIR /app
COPY . /app/bin/worker
RUN pip3 install -r /app/bin/worker/requirements.txt

ENTRYPOINT ["python3", "/app/bin/worker/worker.py"]
