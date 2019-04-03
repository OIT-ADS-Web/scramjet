package psql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/jmoiron/sqlx/types"
	si "gitlab.oit.duke.edu/scholars/staging_importer"
)

type StagingResource struct {
	Id       string         `db:"id"`
	Type     string         `db:"type"`
	Data     types.JSONText `db:"data"`
	IsValid  sql.NullBool   `db:"is_valid"`
	ToDelete sql.NullBool   `db:"to_delete"`
}

// Staging ...
func RetrieveTypeStaging(typeName string) []StagingResource {
	db := GetConnection()
	resources := []StagingResource{}

	// NOTE: this does *not* filter by is_valid so we can try
	// again with previously fails
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	`
	err := db.Select(&resources, sql, typeName)
	if err != nil {
		log.Fatalln(err)
	}
	return resources
}

func RetrieveValidStaging(typeName string) []StagingResource {
	db := GetConnection()
	resources := []StagingResource{}

	// NOTE: this does *not* filter by is_valid so we can try
	// again with previously fails
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND is_valid = TRUE
	`
	err := db.Select(&resources, sql, typeName)
	if err != nil {
		log.Fatalln(err)
	}
	return resources
}

func ListTypeStaging(typeName string, validator si.ValidatorFunc) {
	db := GetConnection()
	resources := []StagingResource{}

	// find ones not already marked invalid ?
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND is_valid != FALSE
	`
	err := db.Select(&resources, sql, typeName)
	for _, element := range resources {
		valid := validator(string(element.Data))
		log.Printf("%v is %t\n", element, valid)
	}
	if err != nil {
		log.Fatalln(err)
	}
}

func FilterTypeStaging(typeName string, validator si.ValidatorFunc) ([]StagingResource, []StagingResource) {
	db := GetConnection()
	resources := []StagingResource{}

	var results = make([]StagingResource, 0)
	var rejects = make([]StagingResource, 0)
	// find ones not already marked invalid ?
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	AND is_valid is not null
	`
	err := db.Select(&resources, sql, typeName)
	for _, element := range resources {
		valid := validator(string(element.Data))
		log.Printf("%v is %t\n", element, valid)
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

func StashTypeStaging(typeName string, docs ...si.Identifiable) error {
	// one at a time?
	/*
		for _, doc := range docs {
			AddStagingResource(doc, doc.Identifier(), typeName)
		}
	*/
	err := BulkAddStaging(typeName, docs...)
	return err
}

func ProcessTypeStaging(typeName string, validator si.ValidatorFunc) {
	valid, rejects := FilterTypeStaging(typeName, validator)
	BatchMarkValidInStaging(valid)
	BatchMarkInvalidInStaging(rejects)
	//return valid, rejects
}

func RetrieveSingleStaging(id string, typeName string) StagingResource {
	db := GetConnection()
	found := StagingResource{}

	// NOTE: this does *not* filter by is_valid - because it's
	// one at a time and would be a re-attempt
	findSQL := `SELECT id, type, data 
	  FROM staging
	  WHERE (id = $1 AND type = $2)`

	err := db.Get(&found, findSQL, id, typeName)

	if err != nil {
		log.Fatalln(err)
	}
	return found
}

/*
    arg := map[string]interface{}{
        "published": true,
        "authors": []{8, 19, 32, 44},
    }
    query, args, err := sqlx.Named("SELECT * FROM articles WHERE published=:published AND author_id IN (:authors)", arg)
    query, args, err := sqlx.In(query, args...)
    query = db.Rebind(query)
		db.Query(query, args...)
*/

/*
select * from staging
where (id, type) IN (('1', 'person'), ('2', 'person'))
*/
func BatchMarkInvalidInStaging(resources []StagingResource) {
	db := GetConnection()

	//type tuple struct {
	//	Id       string
	//	TypeName string
	//}
	var matches = make([][]string, len(resources))

	for _, resource := range resources {
		ary := make([]string, 2)
		ary = append(ary, resource.Id)
		ary = append(ary, resource.Type)
		//matches = append(matches, tuple{Id: resource.Id, TypeName: resource.Type})
		matches = append(matches, ary)
	}
	// limit in statement to 750? - batch up?
	// , typeName string
	tx := db.MustBegin()
	//fmt.Printf(">UPDATE:%v\n", res.Id)
	sql := `UPDATE staging
	  set is_valid = FALSE
		WHERE (id, type) IN (:matches)`
	_, err := tx.NamedExec(sql, resources)

	if err != nil {
		log.Printf(">ERROR(UPDATE):%v", err)
		os.Exit(1)
	}
	tx.Commit()
}

// TODO: should probably batch these when validating and
// mark valid, invalid in groups of 500 or something
func MarkInvalidInStaging(res StagingResource) {
	db := GetConnection()

	tx := db.MustBegin()
	fmt.Printf(">UPDATE:%v\n", res.Id)
	sql := `UPDATE staging
	  set is_valid = FALSE
		WHERE id = :id and type = :type`
	_, err := tx.NamedExec(sql, res)

	if err != nil {
		log.Printf(">ERROR(UPDATE):%v", err)
		os.Exit(1)
	}
	tx.Commit()
}

// needs to do this
/*
select * from staging
where (id, type) IN (('1', 'person'), ('2', 'person'))

(ugly way to do it:)
s := fmt.Sprintf("('%s', '%s')", t.Id, t.TypeName)

fmt.Println(strings.Join(matches, ", "))

buf := bytes.NewBufferString("UPDATE staging set is_valid = TRUE WHERE (id, type) IN(")
for _, v := range matches {
    if i > 0 {
        buf.WriteString("),")
    }
    if _, err := strconv.Atoi(v); err != nil {
        panic("Not number!")
    }
		buf.WriteString(v)
		//buf.WriteString(")")
}
buf.WriteString(")")

*/
func BatchMarkValidInStaging(resources []StagingResource) {
	// NOTE: this would need to only do 500 at a time
	// because of SQL IN clause limit
	db := GetConnection()

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

	tx := db.MustBegin()
	_, err := tx.Exec(sql)

	if err != nil {
		log.Printf(">ERROR(UPDATE):%v", err)
		// TODO: shouldn't exit in library
		os.Exit(1)
	}
	tx.Commit()
}

func MarkValidInStaging(res StagingResource) {
	db := GetConnection()

	tx := db.MustBegin()
	fmt.Printf(">UPDATE:%v\n", res.Id)
	sql := `UPDATE staging
	  set is_valid = TRUE 
		WHERE id = :id and type = :type`
	_, err := tx.NamedExec(sql, res)

	if err != nil {
		log.Printf(">ERROR(UPDATE):%v", err)
		os.Exit(1)
	}
	tx.Commit()
}

func DeleteFromStaging(res StagingResource) {
	db := GetConnection()
	sql := `DELETE from staging WHERE id = :id AND type = :type`

	tx := db.MustBegin()
	tx.NamedExec(sql, res)

	log.Println(sql)
	err := tx.Commit()
	if err != nil {
		log.Printf(">ERROR(DELETE):%v", err)
		os.Exit(1)
	}
}

func StagingTableExists() bool {
	var exists bool
	db := GetConnection()
	catalog := GetDbName()
	// FIXME: not sure this is right
	sqlExists := `SELECT EXISTS (
        SELECT 1
        FROM   information_schema.tables 
        WHERE  table_catalog = $1
        AND    table_name = 'staging'
    )`
	err := db.QueryRow(sqlExists, catalog).Scan(&exists)
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

	db := GetConnection()
	tx := db.MustBegin()
	tx.MustExec(sql)

	err := tx.Commit()
	if err != nil {
		log.Fatalf("ERROR(CREATE):%v", err)
	}
}

func ClearAllStaging() {
	db := GetConnection()
	sql := `DELETE from staging`
	tx := db.MustBegin()
	tx.MustExec(sql)

	log.Println(sql)
	err := tx.Commit()
	if err != nil {
		log.Fatalf(">ERROR(DELETE):%v", err)
	}
}

func ClearStagingType(typeName string) {
	db := GetConnection()
	sql := `DELETE from staging`

	sql += fmt.Sprintf(" WHERE type='%s'", typeName)

	tx := db.MustBegin()
	tx.MustExec(sql)

	log.Println(sql)
	err := tx.Commit()
	if err != nil {
		log.Fatalf(">ERROR(DELETE):%v", err)
	}
}

// only add (presumed existence already checked)
func AddStagingResource(obj interface{}, id string, typeName string) {
	fmt.Printf(">ADD:%v\n", id)
	db := GetConnection()

	str, err := json.Marshal(obj)
	if err != nil {
		log.Fatalln(err)
	}

	res := &StagingResource{Id: id, Type: typeName, Data: str}

	tx := db.MustBegin()
	sql := `INSERT INTO STAGING (id, type, data) 
	      VALUES (:id, :type, :data)`
	_, err = tx.NamedExec(sql, res)
	if err != nil {
		log.Fatalf(">ERROR(INSERT):%v\n", err)
	}
	tx.Commit()
}

func SaveStagingResource(obj si.Identifiable, typeName string) {
	db := GetConnection()

	str, err := json.Marshal(obj)
	if err != nil {
		log.Fatalln(err)
	}

	found := &StagingResource{}
	res := &StagingResource{Id: obj.Identifier(), Type: typeName, Data: str}

	findSql := `SELECT id, type, data FROM staging
	  WHERE (id = $1 AND type = $2)`

	err = db.Get(&found, findSql, obj.Identifier(), typeName)

	tx := db.MustBegin()
	if err != nil {
		// NOTE: assuming the error means it doesn't exist
		fmt.Printf(">ADD:%v\n", res.Id)
		sql := `INSERT INTO staging (id, type, data) 
	      VALUES (:id, :type, :data)`
		_, err := tx.NamedExec(sql, res)
		if err != nil {
			log.Fatalf(">ERROR(INSERT):%v\n", err)
		}
	} else {
		fmt.Printf(">UPDATE:%v\n", found.Id)
		sql := `UPDATE staging
	    set id = id, 
		type = :type, 
		data = :data
		WHERE id = :id and type = :type`
		_, err := tx.NamedExec(sql, res)

		if err != nil {
			log.Fatalf(">ERROR(UPDATE):%v\n", err)
		}
	}
	tx.Commit()
}

func StagingResourceExists(uri string, typeName string) bool {
	var exists bool
	db := GetConnection()
	sqlExists := `SELECT EXISTS (SELECT id FROM staging where (id = $1 AND type =$2))`
	err := db.Get(&exists, sqlExists, uri, typeName)
	if err != nil {
		//log.Fatalf(">ERROR(EXISTS):%v\n", err)
		return false
	}
	return exists
}

// should probably prepare statements beforehand
// https://github.com/andreiavrammsd/go-postgresql-batch-operations
//
// stole code from here:
//https://stackoverflow.com/questions/12486436/

func unique(idSlice []si.Identifiable) []si.Identifiable {
	keys := make(map[si.Identifiable]bool)
	list := []si.Identifiable{}
	for _, entry := range idSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

/*
func removeNulls(idSlice []si.Identifiable) []si.Identifiable {
	list := []si.Identifiable{}
	for _, entry := range idSlice {
		fmt.Printf("identifer=%v\n", entry.Identifier())
		if entry.Identifier() != "" {
			list = append(list, entry)
		}
	}
	return list
}
*/

func BulkAddStaging(typeName string, items ...si.Identifiable) error {
	var resources = make([]StagingResource, 0)

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

	fmt.Printf("got %v resources\n", len(resources))

	for _, resource := range resources {
		fmt.Printf("res=%v\n", resource)
	}
	db := GetConnection()
	txn, err := db.Begin()
	if err != nil {
		return errors.Wrap(err, "begin transaction")
	}

	txOK := false
	defer func() {
		if !txOK {
			txn.Rollback()
		}
	}()

	tmpSql := `CREATE TEMPORARY TABLE staging_data_tmp
	  (id text NOT NULL, type text NOT NULL, data json NOT NULL)
	  ON COMMIT DROP
	`
	log.Printf("sql=%s\n", tmpSql)
	_, err = txn.Exec(tmpSql)

	stmt, err := txn.Prepare(pq.CopyIn("staging_data_tmp", "id", "type", "data"))
	for _, res := range resources {
		// 4 times??
		fmt.Println("trying to execute ...")
		_, err = stmt.Exec(res.Id, typeName, res.Data)
		if err != nil {
			fmt.Errorf("%v\n", err)
			return errors.Wrap(err, "loading COPY data")
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		fmt.Errorf("%v\n", err)
		return errors.Wrap(err, "flush COPY data")
	}
	err = stmt.Close()
	if err != nil {
		fmt.Errorf("%v\n", err)
		return errors.Wrap(err, "close COPY stmt")
	}

	sql2 := `INSERT INTO staging (id, type, data)
	  SELECT id, type, data FROM staging_data_tmp
	  ON CONFLICT (id, type) DO UPDATE SET data = EXCLUDED.data
	`
	log.Printf("sql=%s\n", sql2)
	_, err = txn.Exec(sql2)

	if err != nil {
		fmt.Errorf("%v\n", err)
		return errors.Wrap(err, "move from temporary to real table")
	}

	err = txn.Commit()
	if err != nil {
		fmt.Errorf("%v\n", err)
		return errors.Wrap(err, "commit transaction")
	}
	txOK = true
	fmt.Println("transaction okay")
	return nil
}
