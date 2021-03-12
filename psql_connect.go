package scramjet

import (
	"context"
	"fmt"
	"sync"

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
		//	# Example DSN
		//user=jack password=secret host=pg.example.com port=5432 dbname=mydb sslmode=verify-ca pool_max_conns=10

		//# Example URL
		//postgres://jack:secret@pg.example.com:5432/mydb?sslmode=verify-ca&pool_max_conns=10
		connUrl := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
			conf.Database.User, conf.Database.Password, conf.Database.Server,
			uint16(conf.Database.Port), conf.Database.Database)
		config, err := pgxpool.ParseConfig(connUrl)
		if err != nil {
			err = errors.Wrap(dbErr, "Call to pgx.NewConnPool failed")
		}
		config.MaxConns = int32(conf.Database.MaxConnections)
		// not allowed to set aquire timeout anymore?
		//timeout := time.Duration(conf.Database.AcquireTimeout) * time.Second
		//config.BeforeAcquire = func(ctx context.Context, conn *pgx.Conn) error {
		//	// do something with every new connection
		//}

		connPool, dbErr := pgxpool.ConnectConfig(context.Background(), config)
		if dbErr != nil {
			err = errors.Wrap(dbErr, "Call to pgx.NewConnPool failed")
		}
		DBPool = connPool
		Name = conf.Database.Database
	})
	return err
}
