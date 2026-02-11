FROM python:3.8-slim-bullseye

ENV HOME="/root/"

# Base tools and build deps (including jq as requested)
RUN apt-get update \
    && apt-get install -yq apt-utils build-essential curl gcc wget \
    readline-common neovim git zsh zsh-syntax-highlighting zsh-autosuggestions jq python3-pip unzip p7zip libpcap-dev rubygems ruby-dev grc ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install ProjectDiscovery tools: subfinder, httpx, dnsx
RUN set -eux; \
    arch="amd64"; \
    case "$(uname -m)" in \
      x86_64) arch="amd64" ;; \
      aarch64|arm64) arch="arm64" ;; \
    esac; \
    subfinder_ver="2.12.0"; \
    cd /tmp; \
    subfinder_url="https://github.com/projectdiscovery/subfinder/releases/download/v${subfinder_ver}/subfinder_${subfinder_ver}_linux_${arch}.tar.gz"; \
    echo "Downloading ${subfinder_url}"; \
    curl -sSL "$subfinder_url" -o subfinder.tar.gz; \
    tar -xzf subfinder.tar.gz; \
    mv subfinder /usr/local/bin/; \
    chmod +x /usr/local/bin/subfinder; \
    rm -rf subfinder.tar.gz LICENSE README.md CHANGELOG.md 2>/dev/null || true; \
    subfinder -version || subfinder -h

RUN set -eux; \
    arch="amd64"; \
    case "$(uname -m)" in \
      x86_64) arch="amd64" ;; \
      aarch64|arm64) arch="arm64" ;; \
    esac; \
    httpx_ver="1.8.1"; \
    cd /tmp; \
    httpx_url="https://github.com/projectdiscovery/httpx/releases/download/v${httpx_ver}/httpx_${httpx_ver}_linux_${arch}.tar.gz"; \
    echo "Downloading ${httpx_url}"; \
    curl -sSL "$httpx_url" -o httpx.tar.gz; \
    tar -xzf httpx.tar.gz; \
    mv httpx /usr/local/bin/; \
    chmod +x /usr/local/bin/httpx; \
    rm -rf httpx.tar.gz LICENSE README.md CHANGELOG.md 2>/dev/null || true; \
    httpx -version || httpx -h

RUN set -eux; \
    arch="amd64"; \
    case "$(uname -m)" in \
      x86_64) arch="amd64" ;; \
      aarch64|arm64) arch="arm64" ;; \
    esac; \
    dnsx_ver="1.2.3"; \
    cd /tmp; \
    dnsx_url="https://github.com/projectdiscovery/dnsx/releases/download/v${dnsx_ver}/dnsx_${dnsx_ver}_linux_${arch}.tar.gz"; \
    echo "Downloading ${dnsx_url}"; \
    curl -sSL "$dnsx_url" -o dnsx.tar.gz; \
    tar -xzf dnsx.tar.gz; \
    mv dnsx /usr/local/bin/; \
    chmod +x /usr/local/bin/dnsx; \
    rm -rf dnsx.tar.gz LICENSE README.md CHANGELOG.md 2>/dev/null || true; \
    dnsx -version || dnsx -h

# Application code: use this repo's worker code, no Axiom
WORKDIR /app
COPY . /app/bin/worker
RUN pip3 install -r /app/bin/worker/requirements.txt

ENTRYPOINT ["python3", "/app/bin/worker/worker.py"]
