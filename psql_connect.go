package staging_importer

import (
	"fmt"
	"log"
	"sync"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var connectOnce sync.Once

var Database *sqlx.DB
var Name string

func GetConnection() *sqlx.DB {
	return Database
}

func GetDbName() string {
	return Name
}

// FIXME: don't know whether this is worth the
// 'once' stuff or not?
func MakeConnection(conf Config) error {
	var err error
	connectOnce.Do(func() {
		psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
			"password=%s dbname=%s sslmode=disable",
			conf.Database.Server, conf.Database.Port,
			conf.Database.User, conf.Database.Password,
			conf.Database.Database)

		fmt.Printf("trying to connect to: %s\n", psqlInfo)
		db, dbErr := sqlx.Open("postgres", psqlInfo)
		if dbErr != nil {
			log.Println("m=GetPool,msg=connection has failed", err)
		}

		Database = db
		// NOTE: just needed to check for table existence, probably better way
		Name = conf.Database.Database
		err = dbErr
	})
	return err
}
