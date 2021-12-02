#!/bin/bash

GOOS=linux GOARCH=amd64 go build && \
	scp nebula-stresser jepsen:/root/src && \
	scp nebula-stresser jepsen:/root/nebula-cluster/bin

