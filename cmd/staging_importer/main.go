package main

import (
	"fmt"
	"log"
	"os"

	"github.com/namsral/flag"
	sj "gitlab.oit.duke.edu/scholars/staging_importer"
)

// make this a server?
func main() {
	var conf sj.Config

	dbServer := flag.String("DB_SERVER", "", "database server")
	dbPort := flag.Int("DB_PORT", 0, "database port")
	dbDatabase := flag.String("DB_DATABASE", "", "database database")
	dbUser := flag.String("DB_USER", "", "database user")
	dbPassword := flag.String("DB_PASSWORD", "", "database password")
	dbMaxConnections := flag.Int("DB_MAX_CONNECTIONS", 1, "database maximum pool conections")
	dbAquireTimeout := flag.Int("DB_ACQUIRE_TIMEOUT", 30, "how many seconds to wait to get connection")

	flag.Parse()

	if len(*dbServer) == 0 && len(*dbUser) == 0 {
		log.Fatal("database credentials need to be set")
	} else {
		database := sj.DatabaseInfo{
			Server:         *dbServer,
			Database:       *dbDatabase,
			Password:       *dbPassword,
			Port:           *dbPort,
			User:           *dbUser,
			MaxConnections: *dbMaxConnections,
			AcquireTimeout: *dbAquireTimeout,
		}
		conf = sj.Config{
			Database: database,
		}
	}

	if err := sj.MakeConnectionPool(conf); err != nil {
		fmt.Printf("could not establish postgresql connection %s\n", err)
		os.Exit(1)
	}

	if !sj.StagingTableExists() {
		fmt.Println("staging table not found")
		sj.MakeStagingSchema()
	}
	if !sj.ResourceTableExists() {
		fmt.Println("resources table not found")
		sj.MakeResourceSchema()
	}

	defer sj.DBPool.Close()
}
