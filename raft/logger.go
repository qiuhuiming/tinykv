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
	logger *log.Logger
	level  loggerLevel
	prefix string
}

func newLogger(prefix string, level loggerLevel) *logger {
	return &logger{
		logger: log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds),
		prefix: prefix,
		level:  level,
	}
}

func (l *logger) levelf(levelString string, format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	l.logger.Printf("%s [%s] %s", l.prefix, levelString, msg)
}

func (l *logger) Tracef(format string, a ...interface{}) {
	if l.level > levelTrace {
		return
	}
	l.levelf("TRACE", format, a...)
}

func (l *logger) Debugf(format string, a ...interface{}) {
	if l.level > levelDebug {
		return
	}
	l.levelf("DEBUG", format, a...)
}

func (l *logger) Infof(format string, a ...interface{}) {
	if l.level > levelInfo {
		return
	}
	l.levelf("INFO", format, a...)
}

func (l *logger) Warnf(format string, a ...interface{}) {
	if l.level > levelWarn {
		return
	}
	l.levelf("WARN", format, a...)
}

func (l *logger) Errorf(format string, a ...interface{}) {
	if l.level > levelError {
		return
	}
	l.levelf("ERROR", format, a...)
}

func (l *logger) Panicf(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	l.logger.Panicf("%s [%s] %s", l.prefix, "PANIC", msg)
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
