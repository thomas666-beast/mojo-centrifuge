FROM perl:5.38-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential libssl-dev zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY cpanfile ./
RUN cpanm --installdeps --notest .

COPY . .
EXPOSE 3000
CMD ["perl", "-Ilib", "example.pl", "daemon", "-l", "http://*:3000"]
