package staging_importer

import (
	//"context"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	//"github.com/jackc/pgx"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

// NOTE: just making json []byte instead of pgtype.JSON
type StagingResource struct {
	Id       string       `db:"id"`
	Type     string       `db:"type"`
	Data     []byte       `db:"data"`
	IsValid  sql.NullBool `db:"is_valid"`
	ToDelete sql.NullBool `db:"to_delete"`
}

// Staging ...
func RetrieveTypeStaging(typeName string) []StagingResource {
	db := GetPool()
	ctx := context.Background()
	resources := []StagingResource{}

	// NOTE: this does *not* filter by is_valid so we can try
	// again with previously fails
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	`
	rows, err := db.Query(ctx, sql, typeName)

	for rows.Next() {
		var id string
		var typeName string
		var data []byte

		err = rows.Scan(&id, &typeName, &data)
		res := StagingResource{Id: id, Type: typeName, Data: data}
		resources = append(resources, res)

		if err != nil {
			// is this the correct thing to do?
			continue
		}
	}

	if err != nil {
		log.Fatalln(err)
	}
	return resources
}

func RetrieveValidStaging(typeName string) []StagingResource {
	db := GetPool()
	ctx := context.Background()
	resources := []StagingResource{}

	// NOTE: this does *not* filter by is_valid so we can try
	// again with previously fails
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND is_valid = TRUE
	`
	rows, err := db.Query(ctx, sql, typeName)

	for rows.Next() {
		var id string
		var typeName string
		var data []byte

		err = rows.Scan(&id, &typeName, &data)
		res := StagingResource{Id: id, Type: typeName, Data: data}
		resources = append(resources, res)

		if err != nil {
			// is this the correct thing to do?
			continue
		}
	}

	if err != nil {
		log.Fatalln(err)
	}
	return resources
}

func RetrieveInvalidStaging(typeName string) []StagingResource {
	db := GetPool()
	ctx := context.Background()
	resources := []StagingResource{}

	// NOTE: this does *not* filter by is_valid so we can try
	// again with previously fails
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND is_valid = FALSE
	`
	rows, err := db.Query(ctx, sql, typeName)

	for rows.Next() {
		var id string
		var typeName string
		var data []byte

		err = rows.Scan(&id, &typeName, &data)
		res := StagingResource{Id: id, Type: typeName, Data: data}
		resources = append(resources, res)

		if err != nil {
			// is this the correct thing to do?
			continue
		}
	}

	if err != nil {
		log.Fatalln(err)
	}
	return resources
}

func ListTypeStaging(typeName string, validator ValidatorFunc) {
	db := GetPool()
	ctx := context.Background()
	resources := []StagingResource{}

	// find ones not already marked invalid ?
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND is_valid != FALSE
	`
	rows, err := db.Query(ctx, sql, typeName)

	for rows.Next() {
		var id string
		var typeName string
		var data []byte

		err = rows.Scan(&id, &typeName, &data)
		res := StagingResource{Id: id, Type: typeName, Data: data}
		resources = append(resources, res)

		if err != nil {
			// is this the correct thing to do?
			continue
		}
	}

	for _, element := range resources {
		valid := validator(string(element.Data))
		log.Printf("%v is %t\n", element, valid)
	}
	if err != nil {
		log.Fatalln(err)
	}
}

func FilterTypeStaging(typeName string, validator ValidatorFunc) ([]StagingResource, []StagingResource) {
	db := GetPool()
	ctx := context.Background()
	resources := []StagingResource{}

	var results = make([]StagingResource, 0)
	var rejects = make([]StagingResource, 0)

	// find ones not already marked invalid ?
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND is_valid is not null
	`

	rows, err := db.Query(ctx, sql, typeName)

	// NOTE: sqlx reads straight into array of structs
	// is kind of easier
	for rows.Next() {
		var id string
		var typeName string
		var data []byte

		err = rows.Scan(&id, &typeName, &data)
		res := StagingResource{Id: id, Type: typeName, Data: data}
		resources = append(resources, res)

		if err != nil {
			// is this the correct thing to do?
			continue
		}
	}

	for _, element := range resources {
		valid := validator(string(element.Data))
		if valid {
			results = append(results, element)
		} else {
			rejects = append(rejects, element)
		}
	}
	if err != nil {
		log.Fatalln(err)
	}
	return results, rejects
}

func StashTypeStaging(typeName string, docs ...Identifiable) error {
	// allow one at a time to debug?
	/*
		for _, doc := range docs {
			AddStagingResource(doc, doc.Identifier(), typeName)
		}
	*/
	err := BulkAddStaging(typeName, docs...)
	return err
}

func ProcessTypeStaging(typeName string, validator ValidatorFunc) {
	valid, rejects := FilterTypeStaging(typeName, validator)
	BatchMarkValidInStaging(valid)
	BatchMarkInvalidInStaging(rejects)
}

func RetrieveSingleStaging(id string, typeName string) StagingResource {
	db := GetPool()
	ctx := context.Background()
	var found StagingResource

	// NOTE: this does *not* filter by is_valid - because it's
	// one at a time and would be a re-attempt
	findSQL := `SELECT id, type, data 
	  FROM staging
	  WHERE (id = $1 AND type = $2)`

	row := db.QueryRow(ctx, findSQL, id, typeName)
	err := row.Scan(&found)

	if err != nil {
		log.Fatalln(err)
	}
	return found
}

func BatchMarkInvalidInStaging(resources []StagingResource) {
	chunked := chunkedStaging(resources, 500)
	for _, chunk := range chunked {
		batchMarkInvalidInStaging(chunk)
	}
}

// made lowercase same name to not export
func batchMarkInvalidInStaging(resources []StagingResource) (err error) {
	// NOTE: this would need to only do 500 at a time
	// because of SQL IN clause limit
	db := GetPool()
	ctx := context.Background()

	// TODO: better ways to do this
	var clauses = make([]string, 0)

	for _, resource := range resources {
		s := fmt.Sprintf("('%s', '%s')", resource.Id, resource.Type)
		clauses = append(clauses, s)
	}

	inClause := strings.Join(clauses, ", ")

	sql := fmt.Sprintf(`UPDATE staging set is_valid = FALSE WHERE (id, type) IN (
		  %s
		)`, inClause)

	tx, err := db.Begin(ctx)

	if err != nil {
		//log.Printf(">error beginning transaction:%v", err)
		// TODO: shouldn't exit in library
		//os.Exit(1)
		return err
	}
	_, err = tx.Exec(ctx, sql)

	if err != nil {
		//log.Printf(">ERROR(UPDATE):%v", err)
		// TODO: shouldn't exit in library
		//os.Exit(1)
		return err
	}
	err = tx.Commit(ctx)
	if err != nil {
		//log.Printf(">ERROR(UPDATE) - commit:%v", err)
		// TODO: shouldn't exit in library
		//os.Exit(1)
		return err
	}
	return nil
}

// TODO: should probably batch these when validating and
// mark valid, invalid in groups of 500 or something
func MarkInvalidInStaging(res StagingResource) (err error) {
	db := GetPool()
	ctx := context.Background()
	tx, err := db.Begin(ctx)

	if err != nil {
		//log.Printf(">error beginning transaction:%v", err)
		// TODO: shouldn't exit in library
		//os.Exit(1)
		return err
	}

	sql := `UPDATE staging
	  set is_valid = FALSE
		WHERE id = $1 and type = $2`

	_, err = tx.Exec(ctx, sql, res.Id, res.Type)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	return nil
}

//https://stackoverflow.com/questions/35179656/slice-chunking-in-go
func chunkedStaging(resources []StagingResource, chunkSize int) [][]StagingResource {
	var divided [][]StagingResource

	for i := 0; i < len(resources); i += chunkSize {
		end := i + chunkSize

		if end > len(resources) {
			end = len(resources)
		}

		divided = append(divided, resources[i:end])
	}
	return divided
}

func BatchMarkValidInStaging(resources []StagingResource) {
	chunked := chunkedStaging(resources, 500)
	for _, chunk := range chunked {
		batchMarkValidInStaging(chunk)
	}
}

// okay to just not export?
func batchMarkValidInStaging(resources []StagingResource) (err error) {
	// NOTE: this would need to only do 500-750 (or so) at a time
	// because of SQL IN clause limit of 1000
	db := GetPool()
	ctx := context.Background()
	// TODO: better ways to do this
	var clauses = make([]string, 0)

	for _, resource := range resources {
		s := fmt.Sprintf("('%s', '%s')", resource.Id, resource.Type)
		clauses = append(clauses, s)
	}

	inClause := strings.Join(clauses, ", ")

	sql := fmt.Sprintf(`UPDATE staging set is_valid = TRUE WHERE (id, type) IN (
		  %s
		)`, inClause)

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

func MarkValidInStaging(res StagingResource) (err error) {
	db := GetPool()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}

	sql := `UPDATE staging
	  set is_valid = TRUE 
		WHERE id = $1 and type = $2`
	_, err = tx.Exec(ctx, sql, res.Id, res.Type)

	if err != nil {
		return err
	}
	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	return nil
}

func DeleteFromStaging(res StagingResource) (err error) {
	db := GetPool()
	ctx := context.Background()
	sql := `DELETE from staging WHERE id = $1 AND type = $2`

	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, sql, res.Id, res.Type)

	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	return nil
}

// NOTE: could call Fatalf
func StagingTableExists() bool {
	var exists bool
	db := GetPool()
	ctx := context.Background()
	catalog := GetDbName()
	// FIXME: not sure this is right
	sqlExists := `SELECT EXISTS (
        SELECT 1
        FROM   information_schema.tables 
        WHERE  table_catalog = $1
        AND    table_name = 'staging'
    )`
	row := db.QueryRow(ctx, sqlExists, catalog)
	err := row.Scan(&exists)
	if err != nil {
		log.Fatalf("error checking if row exists %v", err)
	}
	return exists
}

// 'type' should match up to a schema
// NOTE: could call Fatalf
func MakeStagingSchema() {
	sql := `create table staging (
        id text NOT NULL,
        type text NOT NULL,
        data json NOT NULL,
		is_valid boolean DEFAULT FALSE,
		to_delete boolean DEFAULT FALSE,
        PRIMARY KEY(id, type)
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

func DropStaging() error {
	db := GetPool()
	ctx := context.Background()
	sql := `DROP table IF EXISTS staging`
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

func ClearAllStaging() (err error) {
	db := GetPool()
	ctx := context.Background()
	sql := `DELETE from staging`
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

func ClearStagingType(typeName string) (err error) {
	db := GetPool()
	ctx := context.Background()
	sql := `DELETE from staging`

	sql += fmt.Sprintf(" WHERE type='%s'", typeName)

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

func ClearStagingTypeDeletes(typeName string) (err error) {
	db := GetPool()
	ctx := context.Background()
	sql := `DELETE from staging`

	sql += fmt.Sprintf(" WHERE type='%s' AND to_delete = TRUE", typeName)

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

// only add (presumed existence already checked)
func AddStagingResource(obj interface{}, id string, typeName string) (err error) {
	db := GetPool()
	ctx := context.Background()
	str, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	res := &StagingResource{Id: id, Type: typeName, Data: str}

	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	sql := `INSERT INTO STAGING (id, type, data) 
	      VALUES ($1, $2, $3)`
	_, err = tx.Exec(ctx, sql, res.Id, res.Type, res.Data)

	if err != nil {
		return err
	}
	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	return nil
}

func SaveStagingResource(obj Identifiable, typeName string) (err error) {
	db := GetPool()
	ctx := context.Background()
	str, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	//var found StagingResource
	res := &StagingResource{Id: obj.Identifier(), Type: typeName, Data: str}

	findSql := `SELECT id FROM staging
	  WHERE (id = $1 AND type = $2)`

	row := db.QueryRow(ctx, findSql, obj.Identifier(), typeName)

	// NOTE: can't scan into structs
	var foundId string
	notFoundError := row.Scan(&foundId)

	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}

	// supposedly no-op if no problems
	defer tx.Rollback(ctx)

	// e.g. if not found???
	if notFoundError != nil {
		sql := `INSERT INTO staging (id, type, data) 
	      VALUES ($1, $2, $3)`
		_, err := tx.Exec(ctx, sql, res.Id, res.Type, res.Data)

		if err != nil {
			return err
		}
	} else {
		sql := `UPDATE staging
	  set id = $1, 
		type = $2, 
		data = $3,
		is_valid = null
		WHERE id = $1 and type = $2`
		_, err = tx.Exec(ctx, sql, res.Id, res.Type, res.Data)

		if err != nil {
			return err
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	return nil
}

func SaveStagingResourceDirect(res StagingResource, typeName string) (err error) {
	db := GetPool()
	ctx := context.Background()
	//str, err := json.Marshal(obj)
	//if err != nil {
	//	return err
	//}

	//var found StagingResource
	//res := &StagingResource{Id: obj.Identifier(), Type: typeName, Data: str}

	findSql := `SELECT id FROM staging
	  WHERE (id = $1 AND type = $2)`

	row := db.QueryRow(ctx, findSql, res.Id, typeName)

	// NOTE: can't scan into structs
	var foundId string
	notFoundError := row.Scan(&foundId)

	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}

	// supposedly no-op if no problems
	defer tx.Rollback(ctx)

	// e.g. if not found???
	if notFoundError != nil {
		sql := `INSERT INTO staging (id, type, data) 
	      VALUES ($1, $2, $3)`
		_, err := tx.Exec(ctx, sql, res.Id, res.Type, res.Data)

		if err != nil {
			return err
		}
	} else {
		sql := `UPDATE staging
	  set id = $1, 
		type = $2, 
		data = $3,
		is_valid = null
		WHERE id = $1 and type = $2`
		_, err = tx.Exec(ctx, sql, res.Id, res.Type, res.Data)

		if err != nil {
			return err
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	return nil
}

// returns false if error - maybe should not
func StagingResourceExists(uri string, typeName string) bool {
	var exists bool
	db := GetPool()
	ctx := context.Background()

	sqlExists := `SELECT EXISTS (SELECT id FROM staging where (id = $1 AND type =$2))`
	err := db.QueryRow(ctx, sqlExists, uri, typeName).Scan(&exists)
	if err != nil {
		return false
	}
	return exists
}

// should probably prepare statements beforehand
// https://github.com/andreiavrammsd/go-postgresql-batch-operations
//
// stole code from here:
//https://stackoverflow.com/questions/12486436/

// NOTE: was getting "widgets_import.Person is not hashable" trying
// to call (even though tests seemed to work) - so changing
// the hash to the Identifier() seemed to fix that
func unique(idSlice []Identifiable) []Identifiable {
	keys := make(map[string]bool)
	list := []Identifiable{}
	for _, entry := range idSlice {
		if _, value := keys[entry.Identifier()]; !value {
			keys[entry.Identifier()] = true
			list = append(list, entry)
		}
	}
	return list
}

/*
func removeNulls(idSlice []Identifiable) []Identifiable {
	list := []Identifiable{}
	for _, entry := range idSlice {
		fmt.Printf("identifer=%v\n", entry.Identifier())
		if entry.Identifier() != "" {
			list = append(list, entry)
		}
	}
	return list
}
*/

// TODO: not sure how this scales with 100,000+ records
func BulkAddStaging(typeName string, items ...Identifiable) error {
	var resources = make([]StagingResource, 0)
	var err error
	ctx := context.Background()
	// NOTE: not sure if these are necessary
	list := unique(items)

	for _, item := range list {
		str, err := json.Marshal(item)
		if err != nil {
			// return? or let continue loop
			continue
		}
		res := &StagingResource{Id: item.Identifier(), Type: typeName, Data: str}
		resources = append(resources, *res)
	}

	db := GetPool()

	tx, err := db.Begin(ctx)
	if err != nil {
		fmt.Printf("error starting transaction =%v\n", err)
	}

	// supposedly no-op if everything okay
	defer tx.Rollback(ctx)

	tmpSql := `CREATE TEMPORARY TABLE staging_data_tmp
	  (id text NOT NULL, type text NOT NULL, data json NOT NULL)
	  ON COMMIT DROP
	`
	_, err = tx.Exec(ctx, tmpSql)

	if err != nil {
		return errors.Wrap(err, "creating temporary table")
	}

	// NOTE: don't commit yet (see ON COMMIT DROP)

	inputRows := [][]interface{}{}
	for _, res := range resources {
		inputRows = append(inputRows, []interface{}{res.Id, res.Type, res.Data})
	}

	_, err = tx.CopyFrom(ctx, pgx.Identifier{"staging_data_tmp"},
		[]string{"id", "type", "data"},
		pgx.CopyFromRows(inputRows))

	if err != nil {
		return err
	}
	sql2 := `INSERT INTO staging (id, type, data)
	  SELECT id, type, data FROM staging_data_tmp
	  ON CONFLICT (id, type) DO UPDATE SET data = EXCLUDED.data
	`

	_, err = tx.Exec(ctx, sql2)

	if err != nil {
		return errors.Wrap(err, "move from temporary to real table")
	}

	err = tx.Commit(ctx)
	if err != nil {
		return errors.Wrap(err, "commit transaction")
	}
	return nil
}

func BulkAddStagingResources(typeName string, resources ...StagingResource) error {
	db := GetPool()
	ctx := context.Background()
	tx, err := db.Begin(ctx)

	if err != nil {
		fmt.Printf("error starting transaction =%v\n", err)
	}

	// supposedly no-op if everything okay
	defer tx.Rollback(ctx)

	tmpSql := `CREATE TEMPORARY TABLE staging_data_tmp
	  (id text NOT NULL, type text NOT NULL, data json NOT NULL)
	  ON COMMIT DROP
	`
	_, err = tx.Exec(ctx, tmpSql)

	if err != nil {
		return errors.Wrap(err, "creating temporary table")
	}

	// NOTE: don't commit yet (see ON COMMIT DROP)

	inputRows := [][]interface{}{}
	for _, res := range resources {
		inputRows = append(inputRows, []interface{}{res.Id, res.Type, res.Data})
	}

	_, err = tx.CopyFrom(ctx, pgx.Identifier{"staging_data_tmp"},
		[]string{"id", "type", "data"},
		pgx.CopyFromRows(inputRows))

	if err != nil {
		return err
	}
	sql2 := `INSERT INTO staging (id, type, data)
	  SELECT id, type, data FROM staging_data_tmp
	  ON CONFLICT (id, type) DO UPDATE SET data = EXCLUDED.data
	`

	_, err = tx.Exec(ctx, sql2)

	if err != nil {
		return errors.Wrap(err, "move from temporary to real table")
	}

	err = tx.Commit(ctx)
	if err != nil {
		return errors.Wrap(err, "commit transaction")
	}
	return nil
}

func BatchMarkDeleteInStaging(resources []StagingResource) (err error) {
	db := GetPool()
	ctx := context.Background()

	chunked := chunkedStaging(resources, 500)
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	// noop if no problems
	defer tx.Rollback(ctx)
	for _, chunk := range chunked {
		// how to deal with chunked error?
		err := batchMarkDeleteInStaging(chunk, tx)
		if err != nil {
			return err
		}
	}
	err = tx.Commit(ctx)
	if err != nil {
		return err
	}

	return nil
}

func batchMarkDeleteInStaging(resources []StagingResource, tx pgx.Tx) (err error) {
	// NOTE: this would need to only do 500-750 (or so) at a time
	// because of SQL IN clause limit of 1000
	//db := GetPool()
	ctx := context.Background()
	// TODO: better ways to do this
	var clauses = make([]string, 0)

	for _, resource := range resources {
		s := fmt.Sprintf("('%s', '%s')", resource.Id, resource.Type)
		clauses = append(clauses, s)
	}

	inClause := strings.Join(clauses, ", ")

	sql := fmt.Sprintf(`UPDATE staging set to_delete = TRUE WHERE (id, type) IN (
		  %s
		)`, inClause)

	//tx, err := db.Begin()
	//if err != nil {
	//	return err
	//}
	_, err = tx.Exec(ctx, sql)

	if err != nil {
		return err
	}
	//err = tx.Commit()
	//if err != nil {
	//	return err
	//}
	return nil
}

func RetrieveDeletedStaging(typeName string) []StagingResource {
	db := GetPool()
	ctx := context.Background()
	resources := []StagingResource{}

	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND to_delete = TRUE
	`
	rows, err := db.Query(ctx, sql, typeName)

	for rows.Next() {
		var id string
		var typeName string
		var data []byte

		err = rows.Scan(&id, &typeName, &data)
		res := StagingResource{Id: id, Type: typeName, Data: data}
		resources = append(resources, res)

		if err != nil {
			// is this the correct thing to do?
			continue
		}
	}

	if err != nil {
		log.Fatalln(err)
	}
	return resources
}

func BulkAddStagingForDelete(typeName string, items ...Identifiable) error {
	var resources = make([]StagingResource, 0)
	var err error
	ctx := context.Background()
	// NOTE: not sure if these are necessary
	list := unique(items)

	for _, item := range list {
		//str, err := json.Marshal(item)
		//if err != nil {
		// return? or let continue loop
		//	continue
		//}
		// NOTE: empty string for data
		res := &StagingResource{Id: item.Identifier(), Type: typeName, Data: []byte("")}
		resources = append(resources, *res)
	}

	db := GetPool()

	tx, err := db.Begin(ctx)
	if err != nil {
		fmt.Printf("error starting transaction =%v\n", err)
	}

	// supposedly no-op if everything okay
	defer tx.Rollback(ctx)

	tmpSql := `CREATE TEMPORARY TABLE staging_data_deletes_tmp
	  (id text NOT NULL, type text NOT NULL, data json NOT NULL, to_delete boolean DEFAULT TRUE)
	  ON COMMIT DROP
	`
	_, err = tx.Exec(ctx, tmpSql)

	if err != nil {
		return errors.Wrap(err, "creating temporary table")
	}

	// NOTE: don't commit yet (see ON COMMIT DROP)
	inputRows := [][]interface{}{}
	for _, res := range resources {
		inputRows = append(inputRows, []interface{}{res.Id, res.Type, res.Data})
	}

	_, err = tx.CopyFrom(ctx, pgx.Identifier{"staging_data_deletes_tmp"},
		[]string{"id", "type", "data", "to_delete"},
		pgx.CopyFromRows(inputRows))

	if err != nil {
		return err
	}
	// NOTE: if it exists, just nulling out the data
	sql2 := `INSERT INTO staging (id, type, data, to_delete)
	  SELECT id, type, data, to_delete FROM staging_data_deletes_tmp
	  ON CONFLICT (id, type) DO UPDATE SET data = EXCLUDED.data, 
	  to_delete = EXCLUDED.to_delete
	`

	_, err = tx.Exec(ctx, sql2)

	if err != nil {
		return errors.Wrap(err, "move from temporary to real table")
	}

	err = tx.Commit(ctx)
	if err != nil {
		return errors.Wrap(err, "commit transaction")
	}
	return nil
}
