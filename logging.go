package scramjet

import (
	"fmt"
)

// simplest possible logger
var logger Logger
var level LogLevel

type LogLevel int

const (
	INFO  LogLevel = 0
	DEBUG LogLevel = 1
)

func SetLogger(log Logger) {
	logger = log
}

func SetLogLevel(lvl LogLevel) {
	level = lvl
}

type Logger interface {
	Info(msg string)
	Debug(msg string)
}

type simpleLogger struct{}

func (*simpleLogger) Info(msg string) {
	if level >= INFO {
		fmt.Println(msg)
	}
}

func (*simpleLogger) Debug(msg string) {
	if level >= DEBUG {
		fmt.Println(msg)
	}
}
