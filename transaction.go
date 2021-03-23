package scramjet

import (
	"context"
	"log"
)

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
//INSERT INTO staging_transactions (is_finished) VALUES (false) RETURNING id;
// start, end ? -
func MakeTransactionSchema() {
	sql := `create table staging_transactions (
        transaction_id SERIAL PRIMARY_KEY,
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

func NewTransactionId() int {
	var transactionId int
	ctx := context.Background()

	sql := `INSERT INTO 
	staging_transactions 
	DEFAULT VALUES 
	RETURNING transaction_id`

	db := GetPool()
	row := db.QueryRow(ctx, sql)
	err := row.Scan(&transactionId)
	if err != nil {
		log.Fatalf("error getting transactionId %v", err)
	}
	return transactionId
}
