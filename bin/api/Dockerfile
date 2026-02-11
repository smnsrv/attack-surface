FROM python:3.8-slim-bullseye

WORKDIR /python-docker

COPY requirements.txt requirements.txt
RUN apt-get update \
    && apt-get install -y build-essential gcc \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install -r requirements.txt

COPY . .

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0", "--port=80"]
