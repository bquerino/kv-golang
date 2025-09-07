package config

import "time"

type OperationMode string

const (
	ModeLeaderless     OperationMode = "leaderless"
	ModeLeaderFollower OperationMode = "leader-follower"
)

type Config struct {
	Mode              OperationMode
	NodeID            string
	Port              string
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
}

func NewConfig() *Config {
	return &Config{
		Mode:              ModeLeaderless, // Default para manter compatibilidade
		ElectionTimeout:   5 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	}
}

func (c *Config) IsLeaderFollowerMode() bool {
	return c.Mode == ModeLeaderFollower
}

func (c *Config) IsLeaderlessMode() bool {
	return c.Mode == ModeLeaderless
}
