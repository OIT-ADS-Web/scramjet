package psql

import (
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"gitlab.oit.duke.edu/scholars/staging_importer/config"
)

var Database *sqlx.DB
var Name string

func GetConnection() *sqlx.DB {
	return Database
}

func GetDbName() string {
	return Name
}

func MakeConnection(conf config.Config) error {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		conf.Database.Server, conf.Database.Port,
		conf.Database.User, conf.Database.Password,
		conf.Database.Database)

	fmt.Printf("trying to connect to: %s\n", psqlInfo)
	db, err := sqlx.Open("postgres", psqlInfo)
	if err != nil {
		log.Println("m=GetPool,msg=connection has failed", err)
	}

	Database = db
	// NOTE: just needed to check for table existence, probably better way
	Name = conf.Database.Database
	return err
}
