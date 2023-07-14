FROM golang:latest

COPY ./mvrplDFS /usr/bin

RUN mkdir -p /data

CMD ["/usr/bin/mvrplDFS", "datanode", "--port", "7000", "--data-location", "/data/"]