package config

// more flexible?
type Config struct {
	Database Database
}

type Database struct {
	Server   string
	Port     int
	Database string
	User     string
	Password string
}
