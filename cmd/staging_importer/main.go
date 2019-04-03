package main

import (
	"fmt"
	"log"
	"os"

	"github.com/namsral/flag"
	"gitlab.oit.duke.edu/scholars/staging_importer/config"
	"gitlab.oit.duke.edu/scholars/staging_importer/psql"
)

func main() {
	var conf config.Config

	dbServer := flag.String("DB_SERVER", "", "database server")
	dbPort := flag.Int("DB_PORT", 0, "database port")
	dbDatabase := flag.String("DB_DATABASE", "", "database database")
	dbUser := flag.String("DB_USER", "", "database user")
	dbPassword := flag.String("DB_PASSWORD", "", "database password")

	flag.Parse()

	if len(*dbServer) == 0 && len(*dbUser) == 0 {
		log.Fatal("database credentials need to be set")
	} else {
		database := config.Database{
			Server:   *dbServer,
			Database: *dbDatabase,
			Password: *dbPassword,
			Port:     *dbPort,
			User:     *dbUser,
		}
		conf = config.Config{
			Database: database,
		}
	}

	if err := psql.MakeConnection(conf); err != nil {
		fmt.Printf("could not establish postgresql connection %s\n", err)
		os.Exit(1)
	}

	//db := psql.GetConnection()
	// FIXME: how to get catalog name?
	//fmt.Printf("resource table? %t\n", psql.ResourceTableExists())

	if !psql.StagingTableExists() {
		fmt.Println("staging table not found")
		psql.MakeStagingSchema()
	}
	if !psql.ResourceTableExists() {
		fmt.Println("resources table not found")
		psql.MakeResourceSchema()
	}

	defer psql.Database.Close()
}
