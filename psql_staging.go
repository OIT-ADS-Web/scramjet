package staging_importer

import (
	//"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jackc/pgx"
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

	resources := []StagingResource{}

	// NOTE: this does *not* filter by is_valid so we can try
	// again with previously fails
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	`
	rows, err := db.Query(sql, typeName)

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

	resources := []StagingResource{}

	// NOTE: this does *not* filter by is_valid so we can try
	// again with previously fails
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND is_valid = TRUE
	`
	rows, err := db.Query(sql, typeName)

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

	resources := []StagingResource{}

	// NOTE: this does *not* filter by is_valid so we can try
	// again with previously fails
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND is_valid = FALSE
	`
	rows, err := db.Query(sql, typeName)

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

	resources := []StagingResource{}

	// find ones not already marked invalid ?
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND is_valid != FALSE
	`
	rows, err := db.Query(sql, typeName)

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

	resources := []StagingResource{}

	var results = make([]StagingResource, 0)
	var rejects = make([]StagingResource, 0)

	// find ones not already marked invalid ?
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND is_valid is not null
	`

	rows, err := db.Query(sql, typeName)

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

	var found StagingResource

	// NOTE: this does *not* filter by is_valid - because it's
	// one at a time and would be a re-attempt
	findSQL := `SELECT id, type, data 
	  FROM staging
	  WHERE (id = $1 AND type = $2)`

	row := db.QueryRow(findSQL, id, typeName)
	err := row.Scan(&found)

	if err != nil {
		log.Fatalln(err)
	}
	return found
}

func BatchMarkInvalidInStaging(resources []StagingResource) {
	chunked := chunked(resources, 500)
	for _, chunk := range chunked {
		batchMarkInvalidInStaging(chunk)
	}
}

// made lowercase same name to not export
func batchMarkInvalidInStaging(resources []StagingResource) {
	// NOTE: this would need to only do 500 at a time
	// because of SQL IN clause limit
	db := GetPool()

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

	tx, err := db.Begin()

	if err != nil {
		log.Printf(">error beginning transaction:%v", err)
		// TODO: shouldn't exit in library
		os.Exit(1)
	}
	_, err = tx.Exec(sql)

	if err != nil {
		log.Printf(">ERROR(UPDATE):%v", err)
		// TODO: shouldn't exit in library
		os.Exit(1)
	}
	err = tx.Commit()
	if err != nil {
		log.Printf(">ERROR(UPDATE) - commit:%v", err)
		// TODO: shouldn't exit in library
		os.Exit(1)
	}
}

// TODO: should probably batch these when validating and
// mark valid, invalid in groups of 500 or something
func MarkInvalidInStaging(res StagingResource) {
	db := GetPool()

	tx, err := db.Begin()

	if err != nil {
		log.Printf(">error beginning transaction:%v", err)
		// TODO: shouldn't exit in library
		os.Exit(1)
	}

	sql := `UPDATE staging
	  set is_valid = FALSE
		WHERE id = $1 and type = $2`

	_, err = tx.Exec(sql, res.Id, res.Type)
	if err != nil {
		log.Printf(">ERROR(UPDATE):%v", err)
		os.Exit(1)
	}
	if err != nil {
		log.Printf(">ERROR(UPDATE) - commit:%v", err)
		// TODO: shouldn't exit in library
		os.Exit(1)
	}
}

//https://stackoverflow.com/questions/35179656/slice-chunking-in-go
func chunked(resources []StagingResource, chunkSize int) [][]StagingResource {
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
	chunked := chunked(resources, 500)
	for _, chunk := range chunked {
		batchMarkValidInStaging(chunk)
	}
}

// okay to just not export?
func batchMarkValidInStaging(resources []StagingResource) {
	// NOTE: this would need to only do 500-750 (or so) at a time
	// because of SQL IN clause limit of 1000
	db := GetPool()

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

	tx, err := db.Begin()
	if err != nil {
		log.Printf(">error beginning transaction:%v", err)
		// TODO: shouldn't exit in library
		os.Exit(1)
	}
	_, err = tx.Exec(sql)

	if err != nil {
		log.Printf(">ERROR(UPDATE):%v", err)
		// TODO: shouldn't exit in library
		os.Exit(1)
	}
	err = tx.Commit()
	if err != nil {
		log.Printf(">error committing transaction:%v", err)
		// TODO: shouldn't exit in library
		os.Exit(1)
	}
}

func MarkValidInStaging(res StagingResource) {
	db := GetPool()

	tx, err := db.Begin()
	if err != nil {
		log.Printf(">error beginning transaction:%v", err)
		// TODO: shouldn't exit in library
		os.Exit(1)
	}

	sql := `UPDATE staging
	  set is_valid = TRUE 
		WHERE id = $1 and type = $2`
	_, err = tx.Exec(sql, res.Id, res.Type)

	if err != nil {
		log.Printf(">ERROR(UPDATE):%v", err)
		os.Exit(1)
	}
	err = tx.Commit()
	if err != nil {
		log.Printf(">error committing transaction:%v", err)
		// TODO: shouldn't exit in library
		os.Exit(1)
	}
}

func DeleteFromStaging(res StagingResource) {
	db := GetPool()

	sql := `DELETE from staging WHERE id = $1 AND type = $2`

	tx, err := db.Begin()
	if err != nil {
		log.Printf(">error beginning transaction:%v", err)
		// TODO: shouldn't exit in library
		os.Exit(1)
	}
	_, err = tx.Exec(sql, res.Id, res.Type)

	if err != nil {
		log.Printf(">ERROR(DELETE):%v", err)
		os.Exit(1)
	}

	err = tx.Commit()
	if err != nil {
		log.Printf(">error commiting transaction:%v", err)
		os.Exit(1)
	}
}

func StagingTableExists() bool {
	var exists bool
	db := GetPool()

	catalog := GetDbName()
	// FIXME: not sure this is right
	sqlExists := `SELECT EXISTS (
        SELECT 1
        FROM   information_schema.tables 
        WHERE  table_catalog = $1
        AND    table_name = 'staging'
    )`
	row := db.QueryRow(sqlExists, catalog)
	err := row.Scan(&exists)
	if err != nil {
		log.Fatalf("error checking if row exists %v", err)
	}
	return exists
}

// 'type' should match up to a schema
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
	tx, err := db.Begin()
	if err != nil {
		log.Printf(">error beginning transaction:%v", err)
		// TODO: shouldn't exit in library
		os.Exit(1)
	}
	// NOTE: supposedly this is no-op if no error
	defer tx.Rollback()

	_, err = tx.Exec(sql)
	if err != nil {
		log.Printf("ERROR(CREATE):%v", err)
	}
	err = tx.Commit()
	if err != nil {
		log.Printf(">error commiting transaction:%v", err)
		os.Exit(1)
	}

}

func ClearAllStaging() {
	db := GetPool()

	sql := `DELETE from staging`
	tx, err := db.Begin()
	if err != nil {
		log.Printf(">error beginning transaction:%v", err)
		// TODO: shouldn't exit in library
		os.Exit(1)
	}
	_, err = tx.Exec(sql)
	if err != nil {
		log.Fatalf(">ERROR(DELETE):%v", err)
	}
	err = tx.Commit()
	if err != nil {
		log.Printf(">error commiting transaction:%v", err)
		os.Exit(1)
	}
}

func ClearStagingType(typeName string) {
	db := GetPool()

	sql := `DELETE from staging`

	sql += fmt.Sprintf(" WHERE type='%s'", typeName)

	tx, err := db.Begin()
	if err != nil {
		log.Printf(">error beginning transaction:%v", err)
		// TODO: shouldn't exit in library
		os.Exit(1)
	}
	_, err = tx.Exec(sql)
	if err != nil {
		log.Fatalf(">ERROR(DELETE):%v", err)
	}

	err = tx.Commit()
	if err != nil {
		log.Printf(">error commiting transaction:%v", err)
		os.Exit(1)
	}
}

// only add (presumed existence already checked)
func AddStagingResource(obj interface{}, id string, typeName string) {
	db := GetPool()

	str, err := json.Marshal(obj)
	if err != nil {
		log.Printf("error adding to staging: %v\n", err)
		os.Exit(1)
	}

	res := &StagingResource{Id: id, Type: typeName, Data: str}

	tx, err := db.Begin()
	if err != nil {
		log.Printf(">error beginning transaction:%v", err)
		// TODO: shouldn't exit in library
		os.Exit(1)
	}
	sql := `INSERT INTO STAGING (id, type, data) 
	      VALUES ($1, $2, $3)`
	_, err = tx.Exec(sql, res.Id, res.Type, res.Data)

	if err != nil {
		log.Fatalf(">ERROR(INSERT):%v\n", err)
	}
	err = tx.Commit()
	if err != nil {
		log.Printf(">error commiting transaction:%v", err)
		os.Exit(1)
	}
}

func SaveStagingResource(obj Identifiable, typeName string) {
	db := GetPool()

	str, err := json.Marshal(obj)
	if err != nil {
		log.Fatalln(err)
	}

	//var found StagingResource
	res := &StagingResource{Id: obj.Identifier(), Type: typeName, Data: str}

	findSql := `SELECT id FROM staging
	  WHERE (id = $1 AND type = $2)`

	row := db.QueryRow(findSql, obj.Identifier(), typeName)

	// NOTE: can't scan into structs
	var foundId string
	notFoundError := row.Scan(&foundId)

	tx, err := db.Begin()
	if err != nil {
		log.Printf(">error beginning transaction:%v", err)
		// TODO: shouldn't exit in library
		os.Exit(1)
	}

	// supposedly no-op if no problems
	defer tx.Rollback()

	// e.g. if not found???
	if notFoundError != nil {
		sql := `INSERT INTO staging (id, type, data) 
	      VALUES ($1, $2, $3)`
		_, err := tx.Exec(sql, res.Id, res.Type, res.Data)

		if err != nil {
			log.Fatalf(">ERROR(INSERT):%v\n", err)
		}
	} else {
		sql := `UPDATE staging
	  set id = $1, 
		type = $2, 
		data = $3,
		is_valid = null
		WHERE id = $1 and type = $2`
		_, err = tx.Exec(sql, res.Id, res.Type, res.Data)

		if err != nil {
			log.Fatalf(">ERROR(UPDATE):%v\n", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Fatalf(">ERROR commiting transaction:%v\n", err)
	}
}

func StagingResourceExists(uri string, typeName string) bool {
	var exists bool
	db := GetPool()

	sqlExists := `SELECT EXISTS (SELECT id FROM staging where (id = $1 AND type =$2))`
	err := db.QueryRow(sqlExists, uri, typeName).Scan(&exists)
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

func unique(idSlice []Identifiable) []Identifiable {
	keys := make(map[Identifiable]bool)
	list := []Identifiable{}
	for _, entry := range idSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
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
	// NOTE: not sure if these are necessary
	list := unique(items)

	for _, item := range list {
		str, err := json.Marshal(item)
		if err != nil {
			log.Fatalln(err)
		}
		res := &StagingResource{Id: item.Identifier(), Type: typeName, Data: str}
		resources = append(resources, *res)
	}

	db := GetPool()

	tx, err := db.Begin()
	if err != nil {
		fmt.Printf("error starting transaction =%v\n", err)
	}

	// supposedly no-op if everything okay
	defer tx.Rollback()

	tmpSql := `CREATE TEMPORARY TABLE staging_data_tmp
	  (id text NOT NULL, type text NOT NULL, data json NOT NULL)
	  ON COMMIT DROP
	`
	_, err = tx.Exec(tmpSql)

	if err != nil {
		fmt.Printf("err bulk adding=%v\n", err)
	}

	// NOTE: don't commit yet (see ON COMMIT DROP)

	inputRows := [][]interface{}{}
	for _, res := range resources {
		inputRows = append(inputRows, []interface{}{res.Id, res.Type, res.Data})
	}

	_, err = tx.CopyFrom(pgx.Identifier{"staging_data_tmp"},
		[]string{"id", "type", "data"},
		pgx.CopyFromRows(inputRows))

	if err != nil {
		log.Fatalf("err copying from=%v\n", err)
	}
	sql2 := `INSERT INTO staging (id, type, data)
	  SELECT id, type, data FROM staging_data_tmp
	  ON CONFLICT (id, type) DO UPDATE SET data = EXCLUDED.data
	`

	_, err = tx.Exec(sql2)

	if err != nil {
		fmt.Printf("%v\n", err)
		return errors.Wrap(err, "move from temporary to real table")
	}

	err = tx.Commit()
	if err != nil {
		fmt.Printf("error commiting: %v\n", err)
		return errors.Wrap(err, "commit transaction")
	}
	return nil
}
