package main

import (
	"flag"
	"fmt"
	"os"

	"ds-gfs-append/internal/config"
	"ds-gfs-append/internal/logger"
	"ds-gfs-append/internal/master"
)

func main() {
	addr := flag.String("addr", "", "master listen address (override config)")
	logpath := flag.String("log", "operation.log", "operation log path")
	metapath := flag.String("meta", "metadata_snapshot.json", "metadata snapshot output path")
	cfgPath := flag.String("config", "configs/config.yaml", "config file path")
	logLevel := flag.String("loglevel", "INFO", "log level: NONE, ERROR, WARN, INFO, DEBUG")
	flag.Parse()

	logger.SetLevelFromString(*logLevel)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load config:", err)
		os.Exit(1)
	}
	listen := *addr
	if listen == "" {
		listen = cfg.MasterAddress()
	}
	mcfg := master.Config{ReplicationFactor: cfg.Chunk.Replication, LeaseDuration: cfg.LeaseDuration()}
	if err := master.RunMaster(listen, *logpath, *metapath, mcfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
