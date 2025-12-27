package config

import (
	"fmt"
	"io/ioutil"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Master struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
	} `yaml:"master"`
	ChunkServer struct {
		BasePort             int    `yaml:"base_port"`
		DataDir              string `yaml:"data_dir"`
		HeartbeatIntervalSec int    `yaml:"heartbeat_interval_sec"`
	} `yaml:"chunkserver"`
	Chunk struct {
		SizeBytes   int64 `yaml:"size_bytes"`
		Replication int   `yaml:"replication"`
	} `yaml:"chunk"`
	Lease struct {
		DurationSec int `yaml:"duration_sec"`
	} `yaml:"lease"`
}

func Load(path string) (*Config, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) MasterAddress() string {
	host := c.Master.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := c.Master.Port
	if port == 0 {
		port = 50051
	}
	return fmt.Sprintf("%s:%d", host, port)
}

func (c *Config) LeaseDuration() time.Duration {
	if c.Lease.DurationSec <= 0 {
		return 60 * time.Second
	}
	return time.Duration(c.Lease.DurationSec) * time.Second
}

func (c *Config) HeartbeatInterval() time.Duration {
	if c.ChunkServer.HeartbeatIntervalSec <= 0 {
		return 5 * time.Second
	}
	return time.Duration(c.ChunkServer.HeartbeatIntervalSec) * time.Second
}
