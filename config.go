package staging_importer

// more flexible?
type Config struct {
	Database DatabaseInfo
}

type DatabaseInfo struct {
	Server   string
	Port     int
	Database string
	User     string
	Password string
}
