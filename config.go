package scramjet

import "log"

// more flexible?
type Config struct {
	Database DatabaseInfo
	Logger   *Logger
	LogLevel LogLevel
}

type DatabaseInfo struct {
	Server         string
	Port           int
	Database       string
	User           string
	Password       string
	MaxConnections int
	AcquireTimeout int
}

var Cfg *Config

func GetConfig() *Config {
	return Cfg
}

func SetConfig(c *Config) {
	Cfg = c
}

func Configure(conf Config) {
	SetConfig(&conf)

	if conf.Logger != nil {
		SetLogger(*conf.Logger)
	} else {
		logger := Logger(&simpleLogger{})
		SetLogger(logger)
	}

	SetLogLevel(conf.LogLevel)

	logger := Logger(&simpleLogger{})
	SetLogger(logger)
	SetLogLevel(INFO)

	err := MakeConnectionPool(conf)
	if err != nil {
		log.Fatalf("could not make connection pool to database %s:%s\n",
			conf.Database.Server, conf.Database.Database)
	}

	// NOTE: these will log.Fatal too
	if !StagingTableExists() {
		MakeStagingSchema()
	}
	if !ResourceTableExists() {
		MakeResourceSchema()
	}
}

func Shutdown() {
	// caller, in theory, still needs to do this -->
	//defer sj.DBPool.Close()
	DBPool.Close()
}
