package scramjet

import (
	"context"
	"fmt"
	"sync"
	"net/url"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

var connectOnce sync.Once

var DBPool *pgxpool.Pool

var Name string

func GetPool() *pgxpool.Pool {
	return DBPool
}

func GetDbName() string {
	return Name
}

// NOTE: Prepared statements can be manually created with the Prepare method.
// However, this is rarely necessary because pgx includes an automatic statement cache by default
func MakeConnectionPool(conf Config) error {
	var err error

	connectOnce.Do(func() {
		var dbErr error
		// NOTE: seems to be necessary for passwords with some special characters
		replacePass := url.QueryEscape(conf.Database.Password)
		connUrl := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
			conf.Database.User, replacePass, conf.Database.Server,
			uint16(conf.Database.Port), conf.Database.Database)
		config, dbErr := pgxpool.ParseConfig(connUrl)
		if dbErr != nil {
			err = errors.Wrap(dbErr, "Call to pgx.NewConnPool failed")
		}
		config.MaxConns = int32(conf.Database.MaxConnections)
		connPool, dbErr := pgxpool.ConnectConfig(context.Background(), config)
		if dbErr != nil {
			err = errors.Wrap(dbErr, "Call to pgx.NewConnPool failed")
		}
		DBPool = connPool
		Name = conf.Database.Database
	})
	return err
}
