FROM python:3.8-slim-bullseye

ENV HOME="/root/"

# Base tools and build deps (including jq and unzip for ProjectDiscovery binaries)
RUN apt-get update \
    && apt-get install -yq apt-utils build-essential curl wget \
    readline-common neovim git zsh zsh-syntax-highlighting zsh-autosuggestions jq python3-pip unzip p7zip libpcap-dev rubygems ruby-dev grc ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install ProjectDiscovery tools from official GitHub release assets
RUN set -eux; \
    arch="amd64"; \
    case "$(uname -m)" in \
      x86_64) arch="amd64" ;; \
      aarch64|arm64) arch="arm64" ;; \
    esac; \
    cd /tmp; \
    # subfinder
    sub_url_amd64="https://github.com/projectdiscovery/subfinder/releases/download/v2.12.0/subfinder_2.12.0_linux_amd64.zip"; \
    sub_url_arm64="https://github.com/projectdiscovery/subfinder/releases/download/v2.12.0/subfinder_2.12.0_linux_arm64.zip"; \
    sub_url="$sub_url_amd64"; \
    if [ "$arch" = "arm64" ]; then sub_url="$sub_url_arm64"; fi; \
    echo "Downloading $sub_url"; \
    curl -sSL "$sub_url" -o subfinder.zip; \
    mkdir -p subfinder-extract; \
    unzip -q subfinder.zip -d subfinder-extract; \
    mv subfinder-extract/subfinder /usr/local/bin/subfinder; \
    chmod +x /usr/local/bin/subfinder; \
    rm -rf subfinder.zip subfinder-extract; \
    subfinder -version || subfinder -h; \
    # httpx
    httpx_url_amd64="https://github.com/projectdiscovery/httpx/releases/download/v1.8.1/httpx_1.8.1_linux_amd64.zip"; \
    httpx_url_arm64="https://github.com/projectdiscovery/httpx/releases/download/v1.8.1/httpx_1.8.1_linux_arm64.zip"; \
    httpx_url="$httpx_url_amd64"; \
    if [ "$arch" = "arm64" ]; then httpx_url="$httpx_url_arm64"; fi; \
    echo "Downloading $httpx_url"; \
    curl -sSL "$httpx_url" -o httpx.zip; \
    mkdir -p httpx-extract; \
    unzip -q httpx.zip -d httpx-extract; \
    mv httpx-extract/httpx /usr/local/bin/httpx; \
    chmod +x /usr/local/bin/httpx; \
    rm -rf httpx.zip httpx-extract; \
    httpx -version || httpx -h; \
    # dnsx
    dnsx_url_amd64="https://github.com/projectdiscovery/dnsx/releases/download/v1.2.3/dnsx_1.2.3_linux_amd64.zip"; \
    dnsx_url_arm64="https://github.com/projectdiscovery/dnsx/releases/download/v1.2.3/dnsx_1.2.3_linux_arm64.zip"; \
    dnsx_url="$dnsx_url_amd64"; \
    if [ "$arch" = "arm64" ]; then dnsx_url="$dnsx_url_arm64"; fi; \
    echo "Downloading $dnsx_url"; \
    curl -sSL "$dnsx_url" -o dnsx.zip; \
    mkdir -p dnsx-extract; \
    unzip -q dnsx.zip -d dnsx-extract; \
    mv dnsx-extract/dnsx /usr/local/bin/dnsx; \
    chmod +x /usr/local/bin/dnsx; \
    rm -rf dnsx.zip dnsx-extract; \
    dnsx -version || dnsx -h

# Application code: use this repo's worker code, no Axiom
WORKDIR /app
COPY . /app/bin/worker
RUN pip3 install -r /app/bin/worker/requirements.txt

ENTRYPOINT ["python3", "/app/bin/worker/worker.py"]
