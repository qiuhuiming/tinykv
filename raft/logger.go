package raft

import (
	"fmt"
	"log"
	"os"
	"strings"
)

type loggerLevel = uint8

const (
	levelTrace = iota
	levelDebug
	levelInfo
	levelWarn
	levelError
	levelNone
)

type logger struct {
	log.Logger
	level loggerLevel
}

func newLogger(id uint64, level loggerLevel) *logger {
	return &logger{
		Logger: *log.New(os.Stdout, fmt.Sprintf("id(%d)", id), log.LstdFlags|log.Lmicroseconds),
		level:  level,
	}
}

func (l *logger) levelf(levelString string, format string, a ...interface{}) {
	l.Printf("[%s] %s", levelString, format, a)
}

func (l *logger) Tracef(format string, a ...interface{}) {
	if l.level > levelTrace {
		return
	}
	l.levelf("TRACE", format, a)
}

func (l *logger) Debugf(format string, a ...interface{}) {
	if l.level > levelDebug {
		return
	}
	l.levelf("DEBUG", format, a)
}

func (l *logger) Infof(format string, a ...interface{}) {
	if l.level > levelInfo {
		return
	}
	l.levelf("INFO", format, a)
}

func (l *logger) Warnf(format string, a ...interface{}) {
	if l.level > levelWarn {
		return
	}
	l.levelf("WARN", format, a)
}

func (l *logger) Errorf(format string, a ...interface{}) {
	if l.level > levelError {
		return
	}
	l.levelf("ERROR", format, a)
}

func readLevelFromEnv() loggerLevel {
	level := os.Getenv("LEVEL")
	level = strings.ToUpper(level)
	switch level {
	case "TRACE":
		return levelTrace
	case "DEBUG":
		return levelDebug
	case "INFO":
		return levelInfo
	case "WARN":
		return levelWarn
	case "ERROR":
		return levelError
	default:
		return levelNone
	}
}
