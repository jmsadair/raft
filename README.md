![build](https://github.com/jmsadair/raft/actions/workflows/build.yml/badge.svg)

# Raft
This library provides a simple, easy-to-understand, and reliable implementation of [Raft](https://en.wikipedia.org/wiki/Raft_(algorithm)) using Go. Raft is a [consensus](https://en.wikipedia.org/wiki/Consensus_(computer_science)) protocol designed to manage replicated logs in a distributed system. Its purpose is to ensure fault-tolerant coordination and consistency among a group of nodes, making it suitable for building reliable systems. Potential use cases include distributed file systems, consistent key-value stores, and service discovery.

# Installation
First, make sure you have Go `1.19` or a higher version installed on your system. You can download and install Go from the official Go website: https://golang.org/. 

Then, install the Raft library by running

```
go get -u github.com/jmsadair/raft
```

Once you have the library installed, you may refer to the [raft package reference page](https://pkg.go.dev/github.com/jmsadair/raft) for basic usage.
