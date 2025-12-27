package logger

import (
	"fmt"
	"log"
	"os"
	"sync"
)

type LogLevel int

const (
	NONE LogLevel = iota
	ERROR
	WARN
	INFO
	DEBUG
)

var (
	currentLevel LogLevel = INFO
	mu           sync.RWMutex
	loggerStdout = log.New(os.Stdout, "", log.LstdFlags)
)

func SetLevel(level LogLevel) {
	mu.Lock()
	defer mu.Unlock()
	currentLevel = level
}

func GetLevel() LogLevel {
	mu.RLock()
	defer mu.RUnlock()
	return currentLevel
}

func SetLevelFromString(level string) {
	switch level {
	case "NONE", "none":
		SetLevel(NONE)
	case "ERROR", "error":
		SetLevel(ERROR)
	case "WARN", "warn", "WARNING", "warning":
		SetLevel(WARN)
	case "INFO", "info":
		SetLevel(INFO)
	case "DEBUG", "debug":
		SetLevel(DEBUG)
	default:
		SetLevel(INFO)
	}
}

func Debug(format string, v ...interface{}) {
	if GetLevel() >= DEBUG {
		loggerStdout.Printf("[DEBUG] "+format, v...)
	}
}

func Info(format string, v ...interface{}) {
	if GetLevel() >= INFO {
		loggerStdout.Printf("[INFO] "+format, v...)
	}
}

func Warn(format string, v ...interface{}) {
	if GetLevel() >= WARN {
		loggerStdout.Printf("[WARN] "+format, v...)
	}
}

func Error(format string, v ...interface{}) {
	if GetLevel() >= ERROR {
		loggerStdout.Printf("[ERROR] "+format, v...)
	}
}

func Fatal(format string, v ...interface{}) {
	loggerStdout.Printf("[FATAL] "+format, v...)
	os.Exit(1)
}

func Printf(format string, v ...interface{}) {
	if GetLevel() >= INFO {
		fmt.Printf(format, v...)
	}
}

func Println(v ...interface{}) {
	if GetLevel() >= INFO {
		fmt.Println(v...)
	}
}
