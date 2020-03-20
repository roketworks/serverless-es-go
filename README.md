# esgo
Serverless EventSourcing with go!

![Build](https://github.com/roketworks/esgo/workflows/Build/badge.svg)
[![GoDoc](https://godoc.org/github.com/roketworks/esgo?status.svg)](https://godoc.org/github.com/roketworks/esgo)
[![codecov](https://codecov.io/gh/roketworks/esgo/branch/master/graph/badge.svg)](https://codecov.io/gh/roketworks/esgo)
[![Go Report Card](https://goreportcard.com/badge/github.com/roketworks/esgo)](https://goreportcard.com/report/github.com/roketworks/esgo)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Froketworks%2Fesgo.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Froketworks%2Fesgo?ref=badge_shield)

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

## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Froketworks%2Fesgo.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Froketworks%2Fesgo?ref=badge_large)