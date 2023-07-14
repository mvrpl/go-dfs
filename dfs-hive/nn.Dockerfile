FROM golang:latest

COPY ./mvrplDFS /usr/bin

EXPOSE 9000

CMD ["/usr/bin/mvrplDFS", "namenode", "--block-size", "10", "--replication-factor", "3"]