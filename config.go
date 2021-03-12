package scramjet

// more flexible?
type Config struct {
	Database DatabaseInfo
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
