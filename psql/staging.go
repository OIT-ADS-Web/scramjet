package psql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"

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

func FilterStagingList(typeName string, validator si.ValidatorFunc) ([]StagingResource, []StagingResource) {
	db := GetConnection()
	resources := []StagingResource{}

	var results = make([]StagingResource, 0)
	var rejects = make([]StagingResource, 0)
	// find ones not already marked invalid ?
	sql := `SELECT id, type, data 
	FROM staging 
	WHERE type = $1
	--AND is_valid != FALSE
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

// TODO: see above (batching)
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

func SaveStagingResource(obj interface{}, id string, typeName string) {
	db := GetConnection()

	str, err := json.Marshal(obj)
	if err != nil {
		log.Fatalln(err)
	}

	found := &StagingResource{}
	res := &StagingResource{Id: id, Type: typeName, Data: str}

	findSql := `SELECT id, type, data FROM staging
	  WHERE (id = $1 AND type = $2)`

	err = db.Get(&found, findSql, id, typeName)

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
