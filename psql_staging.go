package scramjet

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

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

// kind of like dual primary key
func (res StagingResource) Identifier() Identifier {
	return Identifier{res.Id, res.Type}
}

func buildStagingFilterSql(filter Filter) string {
	var fragment string
	if filter.SubFilter != nil {
		sf := filter.SubFilter
		subFragment := fmt.Sprintf(`SELECT data->>'%s' 
		FROM staging 
		WHERE type = '%s' and data->>'%s' = '%s'`, sf.ParentMatch, sf.Typename, sf.MatchField, sf.Value)
		fragment = fmt.Sprintf(`data->>'%s' %s (%s)`, filter.Field, filter.Compare, subFragment)
	} else {
		fragment = fmt.Sprintf(`data->>'%s' %s '%s'`, filter.Field, filter.Compare, filter.Value)
	}
	return fragment
}

func ScanStaging(rows pgx.Rows) ([]StagingResource, error) {
	resources := []StagingResource{}
	var err error

	for rows.Next() {
		var id string
		var typeName string
		var data []byte

		err = rows.Scan(&id, &typeName, &data)
		res := StagingResource{Id: id, Type: typeName, Data: data}
		resources = append(resources, res)

		if err != nil {
			return resources, err
		}
	}
	return resources, nil
}

func RetrieveTypeStagingFiltered(typeName string, filter Filter) ([]StagingResource, error) {
	db := GetPool()
	ctx := context.Background()

	// NOTE: this does *not* filter by is_valid so we can try
	// again with previously fails
	sql := fmt.Sprintf(`SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND %s
	`, buildStagingFilterSql(filter))

	rows, err := db.Query(ctx, sql, typeName)
	if err != nil {
		return nil, err
	}
	return ScanStaging(rows)
}

func RetrieveTypeStaging(typeName string) ([]StagingResource, error) {
	db := GetPool()
	ctx := context.Background()
	logger := GetLogger()

	// NOTE: this does *not* filter by is_valid so we can try
	// again with previously fails
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	`
	logger.Debug(fmt.Sprintf("running sql %s", sql))
	rows, err := db.Query(ctx, sql, typeName)
	logger.Debug(fmt.Sprintf("returned %d rows", rows))

	if err != nil {
		return nil, err
	}
	return ScanStaging(rows)
}

// just in case we need to look at all records there
func RetrieveAllStaging() ([]StagingResource, error) {
	db := GetPool()
	ctx := context.Background()
	logger := GetLogger()

	// NOTE: this does *not* filter by is_valid so we can try
	// again with previously fails
	sql := `SELECT id, type, data FROM staging`

	logger.Debug(fmt.Sprintf("running sql %s", sql))
	rows, err := db.Query(ctx, sql)
	logger.Debug(fmt.Sprintf("returned %d rows", rows))
	if err != nil {
		return nil, err
	}
	return ScanStaging(rows)
}

func RetrieveValidStaging(typeName string) ([]StagingResource, error) {
	db := GetPool()
	ctx := context.Background()
	logger := GetLogger()

	// NOTE: this does *not* filter by is_valid so we can try
	// again with previously fails
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND is_valid = TRUE
	`
	logger.Debug(fmt.Sprintf("running sql %s", sql))
	rows, err := db.Query(ctx, sql, typeName)
	logger.Debug(fmt.Sprintf("returned %d rows", rows))

	if err != nil {
		return nil, err
	}
	return ScanStaging(rows)
}

func RetrieveValidStagingFiltered(typeName string, filter Filter) ([]StagingResource, error) {
	db := GetPool()
	ctx := context.Background()
	logger := GetLogger()

	// NOTE: this does *not* filter by is_valid so we can try
	// again with previously fails
	sql := fmt.Sprintf(`SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND is_valid = TRUE
	AND %s
	`, buildStagingFilterSql(filter))

	logger.Debug(fmt.Sprintf("running sql %s", sql))
	rows, err := db.Query(ctx, sql, typeName)
	logger.Debug(fmt.Sprintf("returned %d rows", rows))

	if err != nil {
		return nil, err
	}
	return ScanStaging(rows)
}

func RetrieveInvalidStaging(typeName string) ([]StagingResource, error) {
	db := GetPool()
	ctx := context.Background()

	// NOTE: this does *not* filter by is_valid so we can try
	// again with previously fails
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND is_valid = FALSE
	`
	rows, err := db.Query(ctx, sql, typeName)
	if err != nil {
		return nil, err
	}
	return ScanStaging(rows)
}

// NOTE: this needs a 'typeName' param because it assumes validator
// is different per type
func FilterTypeStagingByQuery(typeName string,
	filter Filter, validator ValidatorFunc) ([]Identifiable, []Identifiable, error) {
	db := GetPool()
	ctx := context.Background()

	var results = make([]Identifiable, 0)
	var rejects = make([]Identifiable, 0)

	// find ones not already marked invalid ?
	sql := fmt.Sprintf(`SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND is_valid is not null
	AND %s
	`, buildStagingFilterSql(filter))

	// TODO: way to log.debug only sql
	//fmt.Printf("running sql=%s\n", sql)
	rows, err := db.Query(ctx, sql, typeName)
	if err != nil {
		return results, rejects, err
	}
	resources, err := ScanStaging(rows)
	if err != nil {
		return results, rejects, err
	}

	for _, element := range resources {
		valid := validator(string(element.Data))
		if valid {
			results = append(results, element)
		} else {
			rejects = append(rejects, element)
		}
	}
	return results, rejects, nil
}

func FilterTypeStaging(typeName string, validator ValidatorFunc) ([]Identifiable, []Identifiable, error) {
	db := GetPool()
	ctx := context.Background()

	var results = make([]Identifiable, 0)
	var rejects = make([]Identifiable, 0)

	// find ones not already marked invalid ?
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND is_valid is not null
	`

	rows, err := db.Query(ctx, sql, typeName)
	if err != nil {
		return results, rejects, err
	}

	resources, err := ScanStaging(rows)
	if err != nil {
		return results, rejects, err
	}

	for _, element := range resources {
		valid := validator(string(element.Data))
		if valid {
			results = append(results, element)
		} else {
			rejects = append(rejects, element)
		}
	}
	return results, rejects, nil
}

func StashStaging(docs ...Storeable) error {
	err := BulkAddStaging(docs...)
	return err
}

// TODO: no test for this so far
func ProcessTypeStagingFiltered(typeName string, filter Filter, validator ValidatorFunc) error {
	valid, rejects, err := FilterTypeStagingByQuery(typeName, filter, validator)
	if err != nil {
		return err
	}

	err = BatchMarkValidInStaging(valid)
	if err != nil {
		return err
	}
	err = BatchMarkInvalidInStaging(rejects)
	if err != nil {
		return err
	}
	return nil
}

func ProcessTypeStaging(typeName string, validator ValidatorFunc) error {
	valid, rejects, err := FilterTypeStaging(typeName, validator)
	if err != nil {
		return err
	}

	err = BatchMarkValidInStaging(valid)
	if err != nil {
		return err
	}
	err = BatchMarkInvalidInStaging(rejects)
	if err != nil {
		return err
	}
	return nil
}

func ProcessSingleStaging(item Identifiable, validator ValidatorFunc) error {
	id := item.Identifier()
	// TODO: what to do if no record found?
	res, err := RetrieveSingleStaging(id.Id, id.Type)

	if err != nil {
		return err
	}
	valid := validator(string(res.Data))

	var results = make([]Identifiable, 0)
	results = append(results, res)

	if valid {
		return BatchMarkValidInStaging(results)
	} else {
		return BatchMarkInvalidInStaging(results)
	}
}

func RetrieveSingleStaging(id string, typeName string) (StagingResource, error) {
	db := GetPool()
	ctx := context.Background()
	var found StagingResource

	// NOTE: this does *not* filter by is_valid - because it's
	// one at a time and would be a re-attempt
	findSQL := `SELECT id, type, data 
	  FROM staging
	  WHERE (id = $1 AND type = $2)`

	row := db.QueryRow(ctx, findSQL, id, typeName)

	err := row.Scan(&found.Id, &found.Type, &found.Data)

	if err != nil {
		msg := fmt.Sprintf("ERROR: retrieiving single from staging: %s\n", err)
		return found, errors.New(msg)
	}
	return found, nil
}

func RetrieveSingleStagingValid(id string, typeName string) (StagingResource, error) {
	db := GetPool()
	ctx := context.Background()
	var found StagingResource

	findSQL := `SELECT id, type, data 
	  FROM staging
	  WHERE (id = $1 AND type = $2) 
	  AND is_valid = true`

	row := db.QueryRow(ctx, findSQL, id, typeName)
	err := row.Scan(&found.Id, &found.Type, &found.Data)

	if err != nil {
		msg := fmt.Sprintf("ERROR: retrieving single staging valid: %s\n", err)
		return found, errors.New(msg)
	}
	return found, nil
}

func RetrieveSingleStagingDelete(id string, typeName string) (StagingResource, error) {
	db := GetPool()
	ctx := context.Background()
	var found StagingResource

	findSQL := `SELECT id, type, data 
	  FROM staging
	  WHERE (id = $1 AND type = $2) and to_delete = true`

	row := db.QueryRow(ctx, findSQL, id, typeName)
	err := row.Scan(&found.Id, &found.Type, &found.Data)

	if err != nil {
		msg := fmt.Sprintf("ERROR: retrieving single staging delete: %s\n", err)
		return found, errors.New(msg)
	}
	return found, nil
}

func BatchMarkInvalidInStaging(resources []Identifiable) error {
	chunked := chunked(resources, 500)
	for _, chunk := range chunked {
		err := batchMarkInvalidInStaging(chunk)
		if err != nil {
			return errors.Wrap(err, "marking invalid in staging")
		}
	}
	return nil
}

// made lowercase same name to not export
func batchMarkInvalidInStaging(resources []Identifiable) error {
	// NOTE: this would need to only do 500 at a time
	// because of SQL IN clause limit
	db := GetPool()
	ctx := context.Background()

	// stole idea from here:
	// https://stackoverflow.com/questions/71238345/how-to-do-where-in-any-on-multiple-columns-in-golang-with-pq-library
	inSQL, args := "", []interface{}{}
	for i, resource := range resources {
		n := i * 2
		inSQL += fmt.Sprintf("($%d,$%d),", n+1, n+2)
		args = append(args, resource.Identifier().Id, resource.Identifier().Type)
	}
	inSQL = inSQL[:len(inSQL)-1] // drop last ","

	sql := `UPDATE staging set is_valid = FALSE WHERE (id, type) IN (` + inSQL + `)`

	tx, err := db.Begin(ctx)

	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, sql, args...)

	if err != nil {
		return err
	}
	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	return nil
}

// TODO: should probably batch these when validating and
// mark valid, invalid in groups of 500 or something
func MarkInvalidInStaging(res Storeable) error {
	db := GetPool()
	ctx := context.Background()
	tx, err := db.Begin(ctx)

	if err != nil {
		return err
	}

	sql := `UPDATE staging
	  set is_valid = FALSE
		WHERE id = $1 and type = $2`

	_, err = tx.Exec(ctx, sql, res.Identifier().Id, res.Identifier().Type)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	return nil
}

//https://stackoverflow.com/questions/35179656/slice-chunking-in-go
func chunked(resources []Identifiable, chunkSize int) [][]Identifiable {
	var divided [][]Identifiable

	for i := 0; i < len(resources); i += chunkSize {
		end := i + chunkSize

		if end > len(resources) {
			end = len(resources)
		}

		divided = append(divided, resources[i:end])
	}
	return divided
}

func BatchMarkValidInStaging(resources []Identifiable) error {
	var err error
	chunked := chunked(resources, 500)
	for _, chunk := range chunked {
		err = batchMarkValidInStaging(chunk)
		if err != nil {
			msg := fmt.Sprintf("could not break list into chunks %v", err)
			return errors.New(msg)
		}
	}
	return err
}

func batchMarkValidInStaging(resources []Identifiable) error {
	// NOTE: this would need to only do 500-750 (or so) at a time
	// because of SQL IN clause limit of 1000
	db := GetPool()
	ctx := context.Background()

	// stole idea from here:
	// https://stackoverflow.com/questions/71238345/how-to-do-where-in-any-on-multiple-columns-in-golang-with-pq-library
	inSQL, args := "", []interface{}{}
	for i, resource := range resources {
		n := i * 2
		inSQL += fmt.Sprintf("($%d,$%d),", n+1, n+2)
		args = append(args, resource.Identifier().Id, resource.Identifier().Type)
	}
	inSQL = inSQL[:len(inSQL)-1] // drop last ","

	sql := `UPDATE staging set is_valid = TRUE WHERE (id, type) IN (` + inSQL + `)`

	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, sql, args...)

	if err != nil {
		return err
	}
	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	return nil
}

func MarkValidInStaging(res StagingResource) error {
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

func DeleteFromStaging(res StagingResource) error {
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

func ClearAllStaging() error {
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

// call where valid = true? (after transfering to resources)
func ClearStagingType(typeName string) error {
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

// leave the is_valid = false for investigation
func ClearStagingTypeValid(typeName string) error {
	db := GetPool()
	ctx := context.Background()
	sql := `DELETE from staging`

	sql += fmt.Sprintf(" WHERE type='%s' and is_valid = true", typeName)

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

func ClearStagingTypeValidByFilter(typeName string, filter Filter) error {
	db := GetPool()
	ctx := context.Background()
	sql := fmt.Sprintf(`DELETE from staging
        WHERE type = $1
		AND is_valid = true
		AND %s
	`, buildStagingFilterSql(filter))

	// TODO: need way to debug print
	//fmt.Printf("trying to run sql=%s for type=%s\n", sql, typeName)
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, sql, typeName)
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	return nil
}

func ClearStagingTypeDeletes(typeName string) error {
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

func ClearMultipleDeletedFromStaging(items ...Identifiable) error {
	db := GetPool()
	ctx := context.Background()

	// stole idea from here:
	// https://stackoverflow.com/questions/71238345/how-to-do-where-in-any-on-multiple-columns-in-golang-with-pq-library
	inSQL, args := "", []interface{}{}
	for i, resource := range items {
		n := i * 2
		inSQL += fmt.Sprintf("($%d,$%d),", n+1, n+2)
		args = append(args, resource.Identifier().Id, resource.Identifier().Type)
	}
	inSQL = inSQL[:len(inSQL)-1] // drop last ","

	sql := `DELETE from staging WHERE (id, type) IN (` + inSQL + `)`

	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, sql, args...)
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	return nil
}

func ClearDeletedFromStaging(id string, typeName string) error {
	db := GetPool()
	ctx := context.Background()
	sql := `DELETE from staging`

	sql += fmt.Sprintf(" WHERE id = '%s' AND type='%s' AND to_delete = TRUE", id, typeName)

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
func AddStagingResource(obj interface{}, id string, typeName string) error {
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

// is there a need for this function?
func SaveStagingResource(obj Storeable) error {
	db := GetPool()
	ctx := context.Background()
	str, err := json.Marshal(obj.Object())
	if err != nil {
		msg := fmt.Sprintf("cannot marshal json:%s", err)
		return errors.New(msg)
	}

	findSql := `SELECT id FROM staging
	  WHERE (id = $1 AND type = $2)`

	row := db.QueryRow(ctx, findSql, obj.Identifier().Id, obj.Identifier().Type)

	// NOTE: can't scan into structs
	var foundId string
	notFoundError := row.Scan(&foundId)

	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}

	// supposedly no-op if no problems
	defer tx.Rollback(ctx)

	if notFoundError != nil {
		sql := `INSERT INTO staging (id, type, data)
	      VALUES ($1, $2, $3)`
		_, err := tx.Exec(ctx, sql, obj.Identifier().Id, obj.Identifier().Type, str)

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
		_, err = tx.Exec(ctx, sql, obj.Identifier().Id, obj.Identifier().Type, str)

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

func SaveStagingResourceDirect(res StagingResource, typeName string) error {
	db := GetPool()
	ctx := context.Background()

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
func StagingResourceExists(id string, typeName string) bool {
	var exists bool
	db := GetPool()
	ctx := context.Background()

	sqlExists := `SELECT EXISTS (SELECT id FROM staging where (id = $1 AND type =$2))`
	err := db.QueryRow(ctx, sqlExists, id, typeName).Scan(&exists)
	if err != nil {
		return false
	}
	return exists
}

// stole code from here:
//https://stackoverflow.com/questions/12486436/
func unique(idSlice []Identifiable) []Identifiable {
	keys := make(map[Identifier]bool)
	list := []Identifiable{}
	for _, entry := range idSlice {
		if _, value := keys[entry.Identifier()]; !value {
			keys[entry.Identifier()] = true
			list = append(list, entry)
		}
	}
	return list
}

func uniqueObjects(idSlice []Storeable) []Storeable {
	keys := make(map[Identifier]bool)
	list := []Storeable{}
	for _, entry := range idSlice {
		if _, value := keys[entry.Identifier()]; !value {
			keys[entry.Identifier()] = true
			list = append(list, entry)
		}
	}
	return list
}

func BulkAddStaging(items ...Storeable) error {
	var resources = make([]StagingResource, 0)
	var err error
	ctx := context.Background()
	// NOTE: not sure if these are necessary
	list := uniqueObjects(items)

	for _, item := range list {
		str, err := json.Marshal(item.Object())
		if err != nil {
			// TODO: return err? or let continue loop
			continue
		}
		res := StagingResource{Id: item.Identifier().Id, Type: item.Identifier().Type, Data: str}
		resources = append(resources, res)
	}

	db := GetPool()

	tx, err := db.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "starting transaction")
	}

	// supposedly no-op if everything okay
	defer tx.Rollback(ctx)

	stamp := TimestampString()
	tmpSql := fmt.Sprintf(`CREATE TEMPORARY TABLE staging_data_%s
	  (id text NOT NULL, type text NOT NULL, data json NOT NULL)
	  ON COMMIT DROP
	`, stamp)

	_, err = tx.Exec(ctx, tmpSql)

	if err != nil {
		return errors.Wrap(err, "creating temporary table")
	}

	// NOTE: don't commit yet (see ON COMMIT DROP)

	inputRows := [][]interface{}{}
	for _, res := range resources {
		inputRows = append(inputRows, []interface{}{res.Id, res.Type, res.Data})
	}

	_, err = tx.CopyFrom(ctx, pgx.Identifier{fmt.Sprintf("staging_data_%s", stamp)},
		[]string{"id", "type", "data"},
		pgx.CopyFromRows(inputRows))

	if err != nil {
		return errors.Wrap(err, "copying into temp table")
	}
	sql2 := fmt.Sprintf(`INSERT INTO staging (id, type, data)
	  SELECT id, type, data FROM staging_data_%s
	  ON CONFLICT (id, type) DO UPDATE SET data = EXCLUDED.data
	`, stamp)

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

func BulkAddStagingResources(resources ...StagingResource) error {
	db := GetPool()
	ctx := context.Background()
	tx, err := db.Begin(ctx)

	if err != nil {
		return errors.Wrap(err, "starting transaction")
	}

	// supposedly no-op if everything okay
	defer tx.Rollback(ctx)

	stamp := TimestampString()
	tmpSql := fmt.Sprintf(`CREATE TEMPORARY TABLE staging_data_%s
	  (id text NOT NULL, type text NOT NULL, data json NOT NULL)
	  ON COMMIT DROP
	`, stamp)

	_, err = tx.Exec(ctx, tmpSql)

	if err != nil {
		return errors.Wrap(err, "creating temporary table")
	}

	// NOTE: don't commit yet (see ON COMMIT DROP)

	inputRows := [][]interface{}{}
	for _, res := range resources {
		inputRows = append(inputRows, []interface{}{res.Id, res.Type, res.Data})
	}

	_, err = tx.CopyFrom(ctx, pgx.Identifier{fmt.Sprintf("staging_data_%s", stamp)},
		[]string{"id", "type", "data"},
		pgx.CopyFromRows(inputRows))

	if err != nil {
		return errors.Wrap(err, "copying into temp table")
	}
	sql2 := fmt.Sprintf(`INSERT INTO staging (id, type, data)
	  SELECT id, type, data FROM staging_data_%s
	  ON CONFLICT (id, type) DO UPDATE SET data = EXCLUDED.data
	`, stamp)

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

func RetrieveDeletedStaging(typeName string) ([]Identifiable, error) {
	db := GetPool()
	ctx := context.Background()
	resources := []Identifiable{}

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
		if err != nil {
			return resources, errors.Wrap(err, "could not read data")
		}
		res := StagingResource{Id: id, Type: typeName, Data: data}
		resources = append(resources, res)
	}

	if err != nil {
		return resources, err
	}
	return resources, nil
}

func BulkAddStagingForDelete(items ...Identifiable) error {
	var resources = make([]StagingResource, 0)
	var err error
	ctx := context.Background()
	// NOTE: not sure if these are necessary
	list := unique(items)

	for _, item := range list {
		// NOTE: json cannot be blank - so passing through 'blank' json
		blank := []byte(`{}`)
		res := StagingResource{Id: item.Identifier().Id, Type: item.Identifier().Type, Data: blank}
		resources = append(resources, res)
	}

	db := GetPool()

	tx, err := db.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "starting transaction")
	}

	// supposedly no-op if everything okay
	defer tx.Rollback(ctx)

	// note: just defaulting is_valid and to_delete
	stamp := TimestampString()
	tmpSql := fmt.Sprintf(`CREATE TEMPORARY TABLE staging_data_deletes_%s
	  (id text NOT NULL, type text NOT NULL, data json NOT NULL, 
		is_valid boolean DEFAULT FALSE, to_delete boolean DEFAULT TRUE)
	  ON COMMIT DROP
	`, stamp)

	_, err = tx.Exec(ctx, tmpSql)

	if err != nil {
		return errors.Wrap(err, "creating temporary table")
	}

	// NOTE: don't commit yet (see ON COMMIT DROP)
	inputRows := [][]interface{}{}
	for _, res := range resources {
		inputRows = append(inputRows, []interface{}{res.Id, res.Type, res.Data})
	}

	_, err = tx.CopyFrom(ctx, pgx.Identifier{fmt.Sprintf("staging_data_deletes_%s", stamp)},
		[]string{"id", "type", "data"},
		pgx.CopyFromRows(inputRows))

	if err != nil {
		return errors.Wrap(err, "creating copy rows")
	}
	// NOTE: if it exists, just nulling out the data
	sql2 := fmt.Sprintf(`INSERT INTO staging (id, type, data, is_valid, to_delete)
	  SELECT id, type, data, is_valid, to_delete FROM staging_data_deletes_%s
	  ON CONFLICT (id, type) DO UPDATE SET data = EXCLUDED.data,
	  is_valid = EXCLUDED.is_valid, to_delete = EXCLUDED.to_delete
	`, stamp)

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

// NOTE: only used in test - for verification
func StagingDeleteCount(typeName string) int {
	var count int
	ctx := context.Background()
	sql := `SELECT count(*) 
	FROM staging stg
	WHERE type = $1 and to_delete = TRUE`
	db := GetPool()
	row := db.QueryRow(ctx, sql, typeName)
	err := row.Scan(&count)
	if err != nil {
		log.Fatalf("error checking count %v", err)
	}
	return count
}

// just for verification
func StagingCount() int {
	var count int
	ctx := context.Background()
	sql := `SELECT count(*) 
	FROM staging stg`
	db := GetPool()
	row := db.QueryRow(ctx, sql)
	err := row.Scan(&count)
	if err != nil {
		log.Fatalf("error checking count %v", err)
	}
	return count
}
