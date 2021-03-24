package scramjet

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

type Transaction struct {
	TransactionId int       `db:"transaction_id"`
	CreatedAt     time.Time `db:"created_at"`
	//CompletedAt time.Time `db:"completed_at"` // null?
	CompletedAt sql.NullTime `db:"completed_at"`
}

func TransactionTableExists() bool {
	var exists bool
	db := GetPool()
	ctx := context.Background()
	catalog := GetDbName()
	sqlExists := `SELECT EXISTS (
        SELECT 1
        FROM   information_schema.tables
        WHERE  table_catalog = $1
        AND    table_name = 'staging_transactions'
    )`
	row := db.QueryRow(ctx, sqlExists, catalog)
	err := row.Scan(&exists)
	if err != nil {
		log.Fatalf("error checking if row exists %v", err)
	}
	return exists
}

// how to get value ... example:
func MakeTransactionSchema() {
	sql := `create table staging_transactions (
        transaction_id SERIAL PRIMARY KEY,
		created_at TIMESTAMP DEFAULT NOW(),
		completed_at TIMESTAMP,
		is_finished boolean DEFAULT FALSE
    )`

	db := GetPool()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		log.Fatalf(">error beginning transaction:%v", err)
	}
	// NOTE: supposedly this is no-op if no error
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, sql)
	if err != nil {
		log.Fatalf("ERROR(CREATE):%v", err)
	}
	err = tx.Commit(ctx)
	if err != nil {
		log.Fatalf(">error commiting transaction:%v", err)
	}
}

// returns transaction id
func NewTransaction() int {
	var transactionId int

	db := GetPool()
	ctx := context.Background()
	// do within db transaction?
	tx, err := db.Begin(ctx)

	if err != nil {
		log.Fatalf("error getting transactionId %v", err)
	}

	sql := `INSERT INTO 
	staging_transactions 
	DEFAULT VALUES 
	RETURNING transaction_id`

	row := tx.QueryRow(ctx, sql)
	err = row.Scan(&transactionId)
	if err != nil {
		log.Fatalf("error getting transactionId %v", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Fatalf("error committing transaction %v", err)
	}
	return transactionId
}

func FinishTransaction(transactionId int) error {
	db := GetPool()
	ctx := context.Background()
	tx, err := db.Begin(ctx)

	if err != nil {
		return err
	}

	sql := `UPDATE staging_transactions 
	set completed_at = NOW()
	WHERE transaction_id = $1`

	_, err = tx.Exec(ctx, sql, transactionId)
	if err != nil {
		return err
	}
	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	return nil
}

func ClearAllTransactions() error {
	db := GetPool()
	ctx := context.Background()
	sql := `DELETE from staging_transactions`

	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, sql)

	if err != nil {
		return err
	}
	err = tx.Commit(ctx)

	if err != nil {
		return err
	}
	return nil
}

func TransactionCount() int {
	var count int
	ctx := context.Background()
	sql := `SELECT count(*) FROM staging_transactions tr`
	db := GetPool()
	row := db.QueryRow(ctx, sql)
	err := row.Scan(&count)
	if err != nil {
		log.Fatalf("error checking transaction count %v", err)
	}
	return count
}

func RetrieveSingleTransaction(id int) (Transaction, error) {
	db := GetPool()
	ctx := context.Background()
	var found Transaction

	findSQL := `SELECT transaction_id, created_at, completed_at
	  FROM staging_transactions
	  WHERE transaction_id = $1`

	row := db.QueryRow(ctx, findSQL, id)

	err := row.Scan(&found.TransactionId, &found.CreatedAt, &found.CompletedAt)

	if err != nil {
		msg := fmt.Sprintf("ERROR: retrieiving single from transaction: %s\n", err)
		return found, errors.New(msg)
	}
	return found, nil
}

func ScanTransactions(rows pgx.Rows) ([]Transaction, error) {
	transactions := []Transaction{}
	var err error

	for rows.Next() {
		var id int
		var start time.Time
		var end sql.NullTime

		err = rows.Scan(&id, &start, &end)
		tr := Transaction{TransactionId: id, CreatedAt: start, CompletedAt: end}
		transactions = append(transactions, tr)

		if err != nil {
			return transactions, err
		}
	}
	return transactions, nil
}

// just in case we need to look at all records there
func RetrieveAllTransactions() ([]Transaction, error) {
	db := GetPool()
	ctx := context.Background()

	// NOTE: this does *not* filter by is_valid so we can try
	// again with previously fails
	sql := `SELECT transaction_id, created_at, completed_at FROM staging_transactions`
	rows, err := db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	return ScanTransactions(rows)
}
