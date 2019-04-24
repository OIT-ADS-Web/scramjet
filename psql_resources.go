package staging_importer

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jmoiron/sqlx/types"
)

// this is the raw structure in the database
// two json columms:
// * 'data' can be used for change comparison with hash
// * 'data_b' can be used for searches
type Resource struct {
	Uri       string         `db:"uri"`
	Type      string         `db:"type"`
	Hash      string         `db:"hash"`
	Data      types.JSONText `db:"data"`
	DataB     types.JSONText `db:"data_b"`
	CreatedAt time.Time      `db:"created_at"`
	UpdatedAt time.Time      `db:"updated_at"`
}

//func DeriveUri(u UriAddressable) string { return u.URI() }

// Resources ...

// TODO: could just send in date - leave it up to library user
// to determine how it's figured out
func RetrieveResourceType(typeName string, updates bool) []Resource {
	db := GetConnection()
	resources := []Resource{}

	// need better way to find 'last run'
	var err error
	if updates {
		// TODO: ideally would need to record time last run somewhere
		yesterday := time.Now().AddDate(0, 0, -1)
		rounded := time.Date(yesterday.Year(), yesterday.Month(),
			yesterday.Day(), 0, 0, 0, 0, yesterday.Location())

		sql := `SELECT uri, type, hash, data 
		FROM resources 
		WHERE type =  $1 and updated_at >= $2
      `
		err = db.Select(&resources, sql, typeName, rounded)
	} else {
		sql := `SELECT uri, type, hash, data 
		FROM resources 
		WHERE type =  $1
		`
		err = db.Select(&resources, sql, typeName)
	}

	if err != nil {
		log.Fatalln(err)
	}
	return resources
}

/*
func ListResourceType(typeName string, updates bool) []Resource {
	db := GetConnection()
	resources := []Resource{}

	var err error
	if updates {
		// TODO: ideally would need to record time last run somewhere
		yesterday := time.Now().AddDate(0, 0, -1)

		rounded := time.Date(yesterday.Year(), yesterday.Month(),
			yesterday.Day(), 0, 0, 0, 0, yesterday.Location())

		sql := `SELECT uri, type, hash, data
		FROM resources
		WHERE type = $1
		and updated_at >= $2
      `
		err = db.Select(&resources, sql, typeName, rounded)
	} else {
		sql := `SELECT uri, type, hash, data
		FROM resources
		WHERE type = $1
      `
		err = db.Select(&resources, sql, typeName)
	}

	for _, element := range resources {
		log.Println(element)
		// element is the element from someSlice for where we are
	}
	log.Printf("******* count = %d ********\n", len(resources))

	if err != nil {
		log.Fatalln(err)
	}
	return resources
}
*/

//https://stackoverflow.com/questions/2377881/how-to-get-a-md5-hash-from-a-string-in-golang
func makeHash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

func SaveResource(obj UriAddressable, typeName string) (err error) {
	str, err := json.Marshal(obj)
	if err != nil {
		log.Fatalln(err)
	}

	db := GetConnection()
	hash := makeHash(string(str))

	found := Resource{}
	res := &Resource{Uri: obj.Uri(),
		Type:  typeName,
		Hash:  hash,
		Data:  str,
		DataB: str}

	findSQL := `SELECT uri, type, hash, data, data_b  
	  FROM resources 
		WHERE (uri = $1 AND type = $2)
	`

	err = db.Get(&found, findSQL, obj.Uri(), typeName)

	tx := db.MustBegin()
	// error means not found - sql.ErrNoRows
	if err != nil {
		// NOTE: assuming the error means it doesn't exist
		fmt.Printf(">ADD:%v\n", res.Uri)
		sql := `INSERT INTO resources (uri, type, hash, data, data_b) 
	      VALUES (:uri, :type, :hash, :data, :data_b)`
		_, err := tx.NamedExec(sql, res)
		if err != nil {
			log.Printf(">ERROR(INSERT):%v", err)
			os.Exit(1)
		}
	} else {

		if strings.Compare(hash, found.Hash) == 0 {
			fmt.Printf(">SKIPPING:%v\n", found.Uri)
		} else {
			fmt.Printf(">UPDATE:%v\n", found.Uri)
			sql := `UPDATE resources 
	        set uri = :uri, 
		      type = :type, 
		      hash = :hash, 
		      data = :data, 
		      data_b = :data_b,
		      updated_at = NOW()
		      WHERE uri = :uri and type = :type`
			_, err := tx.NamedExec(sql, res)

			if err != nil {
				log.Printf(">ERROR(UPDATE):%v", err)
				os.Exit(1)
			}
		}
	}

	tx.Commit()
	return err
}

// TODO: the 'table_catalog' changes
func ResourceTableExists() bool {
	var exists bool
	db := GetConnection()
	catalog := GetDbName()
	// FIXME: not sure this is right
	sqlExists := `SELECT EXISTS (
        SELECT 1
        FROM   information_schema.tables 
        WHERE  table_catalog = $1
        AND    table_name = 'resources'
    )`
	err := db.QueryRow(sqlExists, catalog).Scan(&exists)
	if err != nil {
		log.Printf("error checking if row exists %v", err)
		os.Exit(1)
	}
	return exists
}

func MakeResourceSchema() {
	// NOTE: using data AND data_b columns since binary json
	// does NOT keep ordering, it would mess up
	// any hash based comparison, but it could be still be
	// useful for querying
	sql := `create table resources (
        uri text NOT NULL,
        type text NOT NULL,
        hash text NOT NULL,
        data json NOT NULL,
        data_b jsonb NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY(uri, type)
    )`

	db := GetConnection()
	tx := db.MustBegin()
	tx.MustExec(sql)

	err := tx.Commit()
	if err != nil {
		log.Printf("ERROR(CREATE):%v", err)
		os.Exit(1)
	}
}

func ClearAllResources() {
	db := GetConnection()
	sql := `DELETE from resources`
	tx := db.MustBegin()
	tx.MustExec(sql)

	log.Println(sql)
	err := tx.Commit()
	if err != nil {
		log.Printf(">ERROR(DELETE):%v", err)
		os.Exit(1)
	}
}

func ClearResourceType(typeName string) {
	db := GetConnection()
	sql := `DELETE from resources`

	sql += fmt.Sprintf(" WHERE type='%s'", typeName)

	tx := db.MustBegin()
	tx.MustExec(sql)

	log.Println(sql)
	err := tx.Commit()
	if err != nil {
		log.Printf(">ERROR(DELETE):%v", err)
		os.Exit(1)
	}
}
