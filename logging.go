package scramjet

import (
	"fmt"
)

// simplest possible logger
var _logger Logger
var _level LogLevel

type LogLevel int

const (
	INFO  LogLevel = 0
	DEBUG LogLevel = 1
)

func SetLogger(log Logger) {
	_logger = log
}

func GetLogger() Logger {
	return _logger
}

func SetLogLevel(lvl LogLevel) {
	_level = lvl
}

func GetLogLevel() LogLevel {
	return _level
}

type Logger interface {
	Info(msg string)
	Debug(msg string)
}

type simpleLogger struct{}

func (*simpleLogger) Info(msg string) {
	if GetLogLevel() >= INFO {
		fmt.Println(msg)
	}
}

func (*simpleLogger) Debug(msg string) {
	if GetLogLevel() >= DEBUG {
		fmt.Println(msg)
	}
}
