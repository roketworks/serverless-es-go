# esgo
Serverless EventSourcing with go!

![Build](https://github.com/roketworks/esgo/workflows/Build/badge.svg)
[![GoDoc](https://godoc.org/github.com/roketworks/esgo?status.svg)](https://godoc.org/github.com/roketworks/esgo)
[![codecov](https://codecov.io/gh/roketworks/esgo/branch/master/graph/badge.svg)](https://codecov.io/gh/roketworks/esgo)

Esgo is a helper library for Golang >=1.14 that provides the following to help wth implementing basic Event Sourcing in 
AWS serverless architectures with DyanmoDb, SQS & Lambda.

# Install 
`go get github.com/roketworks/esgo`

# Build & Test

## Requirements
* go >= 1.14
* docker
* docker-compose

```shell script
make mod
make setup
make build 
make test
```