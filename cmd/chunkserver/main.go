package main

import (
	"flag"
	"fmt"
	"os"

	"ds-gfs-append/internal/chunkserver"
	"ds-gfs-append/internal/config"
	"ds-gfs-append/internal/logger"
)

func main() {
	listen := flag.String("addr", "", "chunkserver listen address (override config)")
	data := flag.String("data", "", "data directory (override config)")
	masterAddr := flag.String("master", "", "master address (override config)")
	cfgPath := flag.String("config", "configs/config.yaml", "config file path")
	logLevel := flag.String("loglevel", "INFO", "log level: NONE, ERROR, WARN, INFO, DEBUG")
	flag.Parse()

	logger.SetLevelFromString(*logLevel)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load config:", err)
		os.Exit(1)
	}

	addr := *listen
	if addr == "" {
		base := cfg.ChunkServer.BasePort
		if base == 0 {
			base = 6000
		}
		addr = fmt.Sprintf(":%d", base)
	}

	dataDir := *data
	if dataDir == "" {
		dataDir = cfg.ChunkServer.DataDir
		if dataDir == "" {
			dataDir = "data"
		}
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		fmt.Fprintln(os.Stderr, "create data dir:", err)
		os.Exit(1)
	}

	master := *masterAddr
	if master == "" {
		master = cfg.MasterAddress()
	}

	cs := chunkserver.NewChunkServer(addr, dataDir, master, cfg.Chunk.SizeBytes, cfg.HeartbeatInterval())
	if err := cs.Run(addr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
