package staging_importer

import (
	"github.com/pkg/errors"
	"sync"
	"time"

	"github.com/jackc/pgx"
)

var connectOnce sync.Once

var Pool *pgx.ConnPool

var Name string

func GetPool() *pgx.ConnPool {
	return Pool
}

func GetDbName() string {
	return Name
}

var statements = map[string]string{
	"check_if_user_exists": "SELECT exists (SELECT 1 FROM users where username = $1)",
	/*"get_users": `
		SELECT array_to_json(COALESCE(array_agg(t), '{}')) FROM (
			SELECT *, (SELECT up FROM (SELECT firstname, lastname FROM user_profile WHERE user_id=u.id) up) AS profile
			FROM users AS u
			ORDER BY u.id
		) AS t`,
	"create_user":         "INSERT INTO users (username, score) VALUES ($1, $2) RETURNING id, created",
	"create_user_profile": "INSERT INTO user_profile (user_id, firstname, lastname) VALUES ($1, $2, $3)",
	"update_user_score":   "UPDATE users SET score = $2, updated = $3 WHERE id = $1",
	"log_action":          "INSERT INTO actions (description) VALUES ($1)",
	"get_actions": `
		SELECT array_to_json(COALESCE(array_agg(a), '{}')) FROM (
			SELECT id, description, created
			FROM actions
			ORDER BY id
		) AS a
	`,
	*/
}

func MakeConnectionPool(conf Config) error {
	var err error

	connectOnce.Do(func() {
		var dbErr error
		connConfig := pgx.ConnConfig{
			Host:     conf.Database.Server,
			Database: conf.Database.Database,
			User:     conf.Database.User,
			Password: conf.Database.Password,
			Port:     uint16(conf.Database.Port),
		}

		timeout := time.Duration(conf.Database.AcquireTimeout) * time.Second
		connPool, dbErr := pgx.NewConnPool(pgx.ConnPoolConfig{
			ConnConfig:     connConfig,
			AfterConnect:   nil,
			MaxConnections: conf.Database.MaxConnections,
			AcquireTimeout: timeout,
		})

		if dbErr != nil {
			err = errors.Wrap(dbErr, "Call to pgx.NewConnPool failed")
		}
		Pool = connPool
		Name = conf.Database.Database
	})
	return err
}

func PrepareStatements() error {
	//db := GetConnection()
	db := GetPool()

	for name, sql := range statements {
		// TODO: how to pull these out?
		_, err := db.Prepare(name, sql)
		// if put in map - have to remember
		// to call Close() after using
		if err != nil {
			return err
		}
	}

	return nil
}
