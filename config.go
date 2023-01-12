package raft

import "time"

type Config struct {
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	LoggerLevel       string
}
