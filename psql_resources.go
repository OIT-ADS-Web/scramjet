package staging_importer

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/pgtype"
)

// this is the raw structure in the database
// two json columms:
// * 'data' can be used for change comparison with hash
// * 'data_b' can be used for searches
type Resource struct {
	Uri       string       `db:"uri"`
	Type      string       `db:"type"`
	Hash      string       `db:"hash"`
	Data      pgtype.JSON  `db:"data"`
	DataB     pgtype.JSONB `db:"data_b"`
	CreatedAt time.Time    `db:"created_at"`
	UpdatedAt time.Time    `db:"updated_at"`
}

//func DeriveUri(u UriAddressable) string { return u.URI() }

// Resources ...

// TODO: could just send in date - leave it up to library user
// to determine how it's figured out
func RetrieveResourceType(typeName string, updates bool) ([]Resource, error) {
	db := GetPool()
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
		rows, _ := db.Query(sql, typeName, rounded)

		for rows.Next() {
			// TODO: maybe read into struct (value by value)
			// to make this a little less verbose
			var uri string
			var typeName string
			var hash string
			var json pgtype.JSON
			var jsonB pgtype.JSONB

			err = rows.Scan(&uri, &typeName, &hash, &json, &jsonB)
			res := Resource{Uri: uri,
				Type:  typeName,
				Hash:  hash,
				Data:  json,
				DataB: jsonB}
			resources = append(resources, res)

			if err != nil {
				// is this the correct thing to do?
				continue
			}
		}
	} else {
		sql := `SELECT uri, type, hash, data, data_b
		FROM resources 
		WHERE type =  $1
		`
		rows, _ := db.Query(sql, typeName)

		for rows.Next() {
			var uri string
			var typeName string
			var hash string
			var json pgtype.JSON
			var jsonB pgtype.JSONB

			err = rows.Scan(&uri, &typeName, &hash, &json, &jsonB)
			res := Resource{Uri: uri,
				Type:  typeName,
				Hash:  hash,
				Data:  json,
				DataB: jsonB}
			resources = append(resources, res)

			if err != nil {
				// is this the correct thing to do?
				continue
			}
		}
	}

	if err != nil {
		return nil, err
	}
	return resources, nil
}

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

	db := GetPool()

	hash := makeHash(string(str))

	found := Resource{}
	var data pgtype.JSON
	var dataB pgtype.JSONB
	err = data.Set(str)
	err = dataB.Set(str)

	if err != nil {
		return err
	}

	res := &Resource{Uri: obj.Uri(),
		Type:  typeName,
		Hash:  hash,
		Data:  data,
		DataB: dataB}

	findSQL := `SELECT uri, type, hash, data, data_b  
	  FROM resources 
		WHERE (uri = $1 AND type = $2)
	`

	row := db.QueryRow(findSQL, obj.Uri(), typeName)
	notFoundError := row.Scan(&found.Uri, &found.Type)

	tx, err := db.Begin()

	if notFoundError != nil {
		sql := `INSERT INTO resources (uri, type, hash, data, data_b) 
	      VALUES ($1, $2, $3, $4, $5)`
		_, err := tx.Exec(sql, res.Uri, res.Type, res.Hash, &res.Data, &res.DataB)

		if err != nil {
			return err
		}
	} else {

		if strings.Compare(hash, found.Hash) == 0 {
			// some kind of debug level?
			log.Printf(">SKIPPING:%v\n", found.Uri)
		} else {
			log.Printf(">UPDATE:%v\n", found.Uri)
			sql := `UPDATE resources 
	        set uri = $1, 
		      type = $2, 
		      hash = $3, 
		      data = $4, 
		      data_b = $5,
		      updated_at = NOW()
		      WHERE uri = $1 and type = $2`
			_, err := tx.Exec(sql, res.Uri, res.Type, res.Hash, &res.Data, &res.DataB)

			if err != nil {
				return err
			}
		}
	}

	err = tx.Commit()
	return err
}

// TODO: the 'table_catalog' changes
func ResourceTableExists() bool {
	var exists bool
	db := GetPool()

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
		log.Fatalf("error checking if row exists %v", err)
	}
	return exists
}

/* NOTE: this calls Fatalf with errors */
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

	db := GetPool()

	tx, err := db.Begin()
	if err != nil {
		log.Fatalf(">error beginning transaction:%v", err)
	}
	_, err = tx.Exec(sql)

	if err != nil {
		log.Fatalf(">error executing sql:%v", err)
	}

	err = tx.Commit()
	if err != nil {
		log.Fatalf("ERROR(CREATE):%v", err)
	}
}

// TODO: should probably return error -  not have os.Exit

func ClearAllResources() (err error) {
	db := GetPool()
	sql := `DELETE from resources`

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec(sql)

	if err != nil {
		return err
	}
	err = tx.Commit()

	if err != nil {
		return err
	}
	return nil
}

// TODO: should probably return error -  not have os.Exit
func ClearResourceType(typeName string) (err error) {
	db := GetPool()

	sql := `DELETE from resources`
	sql += fmt.Sprintf(" WHERE type='%s'", typeName)

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec(sql)

	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}
