package scramjet

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

// this is the raw structure in the database
// two json columms:
// * 'data' can be used for change comparison with hash
// * 'data_b' can be used for searches
type Resource struct {
	Id        string       `db:"id"`
	Type      string       `db:"type"`
	Hash      string       `db:"hash"`
	Data      pgtype.JSON  `db:"data"`
	DataB     pgtype.JSONB `db:"data_b"`
	CreatedAt time.Time    `db:"created_at"`
	UpdatedAt time.Time    `db:"updated_at"`
}

func (res Resource) Identifier() Identifier {
	return Identifier{res.Id, res.Type}
}

// TODO: could just send in date - leave it up to library user
// to determine how it's figured out
func RetrieveTypeResources(typeName string) ([]Resource, error) {
	db := GetPool()
	resources := []Resource{}
	ctx := context.Background()
	var err error
	sql := `SELECT id, type, hash, data, data_b
		FROM resources 
		WHERE type =  $1
		`
	rows, _ := db.Query(ctx, sql, typeName)

	for rows.Next() {
		var id string
		var typeName string
		var hash string
		var json pgtype.JSON
		var jsonB pgtype.JSONB

		err = rows.Scan(&id, &typeName, &hash, &json, &jsonB)
		res := Resource{Id: id,
			Type:  typeName,
			Hash:  hash,
			Data:  json,
			DataB: jsonB}
		resources = append(resources, res)

		if err != nil {
			return resources, errors.Wrap(err, "cannot scan in resource")
		}
	}

	if err != nil {
		return resources, err
	}
	return resources, nil
}

func RetrieveTypeResourcesLimited(typeName string, limit int) ([]Resource, error) {
	db := GetPool()
	resources := []Resource{}
	ctx := context.Background()
	var err error
	sql := `SELECT id, type, hash, data, data_b
		FROM resources 
		WHERE type =  $1
		LIMIT $2
		`
	rows, _ := db.Query(ctx, sql, typeName, limit)

	for rows.Next() {
		var id string
		var typeName string
		var hash string
		var json pgtype.JSON
		var jsonB pgtype.JSONB

		err = rows.Scan(&id, &typeName, &hash, &json, &jsonB)
		res := Resource{Id: id,
			Type:  typeName,
			Hash:  hash,
			Data:  json,
			DataB: jsonB}
		resources = append(resources, res)

		if err != nil {
			return resources, errors.Wrap(err, "cannot scan in resource")
		}
	}

	if err != nil {
		return resources, err
	}
	return resources, nil
}

// FIXME: boilerplate - just different query
func RetrieveTypeResourcesByQuery(typeName string, filter string) ([]Resource, error) {
	db := GetPool()
	resources := []Resource{}
	ctx := context.Background()
	var err error
	sql := fmt.Sprintf(`SELECT id, type, hash, data, data_b
		FROM resources 
		WHERE type =  $1
		AND %s
		`, filter)

	// TODO: would like a way to log.debug->
	//	fmt.Printf("res-sql=%s\n", sql)
	rows, _ := db.Query(ctx, sql, typeName)

	for rows.Next() {
		var id string
		var typeName string
		var hash string
		var json pgtype.JSON
		var jsonB pgtype.JSONB

		err = rows.Scan(&id, &typeName, &hash, &json, &jsonB)
		res := Resource{Id: id,
			Type:  typeName,
			Hash:  hash,
			Data:  json,
			DataB: jsonB}
		resources = append(resources, res)

		if err != nil {
			return resources, errors.Wrap(err, "cannot scan in resource")
		}
	}

	if err != nil {
		return resources, err
	}
	return resources, nil
}

//https://stackoverflow.com/questions/2377881/how-to-get-a-md5-hash-from-a-string-in-golang
func makeHash(text string) string {
	hasher := md5.New()
	_, err := hasher.Write([]byte(text))
	if err != nil {
		// TODO: right thing to do here?
		log.Fatalln(err)
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

// only does one at a time (not typically used)
func SaveResource(obj Storeable) error {
	ctx := context.Background()
	str, err := json.Marshal(obj.Object())

	if err != nil {
		log.Fatalln(err)
		return err
	}

	db := GetPool()

	hash := makeHash(string(str))

	found := Resource{}
	var data pgtype.JSON
	var dataB pgtype.JSONB
	err = data.Set(str)

	if err != nil {
		return err
	}
	err = dataB.Set(str)
	if err != nil {
		return err
	}

	res := &Resource{Id: obj.Identifier().Id,
		Type:  obj.Identifier().Type,
		Hash:  hash,
		Data:  data,
		DataB: dataB}

	findSQL := `SELECT id, type, hash, data, data_b  
	  FROM resources 
	  WHERE (id = $1 AND type = $2)
	`

	row := db.QueryRow(ctx, findSQL, obj.Identifier().Id, obj.Identifier().Type)
	notFoundError := row.Scan(&found.Id, &found.Type)

	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	// either insert or update
	if notFoundError != nil {
		// TODO: created_at, updated_at
		sql := `INSERT INTO resources (id, type, hash, data, data_b) 
	      VALUES ($1, $2, $3, $4, $5)`
		_, err := tx.Exec(ctx, sql, res.Id, res.Type, res.Hash, &res.Data, &res.DataB)

		if err != nil {
			return err
		}
	} else {

		if strings.Compare(hash, found.Hash) == 0 {
			// some kind of debug level?
			log.Printf(">SKIPPING:%v\n", found.Id)
		} else {
			log.Printf(">UPDATE:%v\n", found.Id)
			sql := `UPDATE resources 
	        set id = $1, 
		      type = $2, 
		      hash = $3, 
		      data = $4, 
		      data_b = $5,
		      updated_at = NOW()
		      WHERE id = $1 and type = $2`
			_, err := tx.Exec(ctx, sql, res.Id, res.Type, res.Hash, &res.Data, &res.DataB)

			if err != nil {
				return err
			}
		}
	}

	err = tx.Commit(ctx)
	// TODO: return :insert or :update (or nil)
	return err
}

// TODO: the 'table_catalog' changes
func ResourceTableExists() bool {
	var exists bool
	ctx := context.Background()
	db := GetPool()

	catalog := GetDbName()
	sqlExists := `SELECT EXISTS (
        SELECT 1
        FROM   information_schema.tables 
        WHERE  table_catalog = $1
        AND    table_name = 'resources'
    )`
	err := db.QueryRow(ctx, sqlExists, catalog).Scan(&exists)
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
        id text NOT NULL,
        type text NOT NULL,
        hash text NOT NULL,
        data json NOT NULL,
        data_b jsonb NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),
		PRIMARY KEY(id, type),
		CONSTRAINT uniq_id_hash UNIQUE (id, type, hash)
    )`
	ctx := context.Background()
	db := GetPool()

	tx, err := db.Begin(ctx)
	if err != nil {
		log.Fatalf(">error beginning transaction:%v", err)
	}
	_, err = tx.Exec(ctx, sql)

	if err != nil {
		log.Fatalf(">error executing sql:%v", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Fatalf("ERROR(CREATE):%v", err)
	}
}

func DropResources() error {
	db := GetPool()
	ctx := context.Background()
	sql := `DROP table IF EXISTS resources`
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

func ClearAllResources() error {
	db := GetPool()
	ctx := context.Background()
	sql := `DELETE from resources`

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

func ClearResourceType(typeName string) error {
	db := GetPool()
	ctx := context.Background()
	sql := `DELETE from resources`
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

// NOTE: only need 'typeName' param for clearing out from staging
func BulkMoveStagingTypeToResources(typeName string, items ...StagingResource) error {
	var resources = make([]Resource, 0)

	var err error
	ctx := context.Background()

	for _, item := range items {
		hash := makeHash(string(item.Data))

		var data pgtype.JSON
		var dataB pgtype.JSONB
		err = data.Set(item.Data)

		if err != nil {
			return err
		}

		err = dataB.Set(item.Data)

		if err != nil {
			return err
		}

		res := &Resource{Id: item.Identifier().Id,
			Type:  item.Identifier().Type,
			Hash:  hash,
			Data:  data,
			DataB: dataB}
		resources = append(resources, *res)
	}

	db := GetPool()

	tx, err := db.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "starting transaction")
	}

	// supposedly no-op if everything okay
	defer tx.Rollback(ctx)

	tmpSql := `CREATE TEMPORARY TABLE resource_data_tmp
	  (id text NOT NULL, type text NOT NULL, hash text NOT NULL,
		data json NOT NULL, data_b jsonb NOT NULL,
		PRIMARY KEY(id, type)
	  )
	  ON COMMIT DROP
	`
	_, err = tx.Exec(ctx, tmpSql)

	if err != nil {
		return errors.Wrap(err, "creating temporary table")
	}

	// NOTE: don't commit yet (see ON COMMIT DROP)
	inputRows := [][]interface{}{}
	for _, res := range resources {
		x := []byte{}
		readError := res.Data.AssignTo(&x)
		if readError != nil {
			return errors.Wrap(err, fmt.Sprintf("could not read json data:%s", res.Identifier()))
		}
		y := []byte{}
		readError = res.DataB.AssignTo(&y)

		if readError != nil {
			return errors.Wrap(err, fmt.Sprintf("could not read json data:%s", res.Identifier()))
		}
		inputRows = append(inputRows, []interface{}{res.Id,
			res.Type,
			res.Hash,
			x,
			y})
	}

	_, err = tx.CopyFrom(ctx, pgx.Identifier{"resource_data_tmp"},
		[]string{"id", "type", "hash", "data", "data_b"},
		pgx.CopyFromRows(inputRows))

	if err != nil {
		return errors.Wrap(err, "copying records into into temporary table")
	}

	sqlUpsert := `INSERT INTO resources (id, type, hash, data, data_b)
	  SELECT id, type, hash, data, data_b 
	  FROM resource_data_tmp
		
	  ON CONFLICT (id, type) DO UPDATE SET 
	    data = EXCLUDED.data, 
		data_b = EXCLUDED.data_b, 
		hash = EXCLUDED.hash,
		updated_at = CASE 
		  WHEN resources.hash != EXCLUDED.hash THEN NOW()
		  ELSE resources.updated_at
		END
	`
	_, err = tx.Exec(ctx, sqlUpsert)
	if err != nil {
		return errors.Wrap(err, "move from temporary to real table")
	}

	err = tx.Commit(ctx)
	if err != nil {
		return errors.Wrap(err, "commit transaction")
	}
	err = ClearStagingTypeValid(typeName)
	if err != nil {
		return errors.Wrap(err, "clearing staging table")
	}
	return nil
}

func BatchDeleteStagingFromResources(resources ...Identifiable) error {
	db := GetPool()
	ctx := context.Background()
	chunked := chunked(resources, 500)
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	// noop if no problems
	defer tx.Rollback(ctx)
	for _, chunk := range chunked {
		// how best to deal with chunked errors?
		// cancel entire transaction?
		err := batchDeleteStagingFromResources(ctx, chunk, tx)
		if err != nil {
			return errors.Wrap(err, "deleting staging from resources")
		}
	}
	err = tx.Commit(ctx)
	if err != nil {
		return errors.Wrap(err, "committing transaction")
	}
	return nil
}

// how to enusure staging-resource IS identifiable
func batchDeleteStagingFromResources(ctx context.Context, resources []Identifiable, tx pgx.Tx) error {
	var clauses = make([]string, 0)
	for _, resource := range resources {
		s := fmt.Sprintf("('%s', '%s')", resource.Identifier().Id, resource.Identifier().Type)
		clauses = append(clauses, s)
	}

	inClause := strings.Join(clauses, ", ")

	sql := fmt.Sprintf(`DELETE from resources WHERE (id, type) IN (
		%s
	)`, inClause)

	_, err := tx.Exec(ctx, sql)

	if err != nil {
		return err
	}
	return nil
}

func BatchDeleteResourcesFromResources(resources ...Identifiable) error {
	db := GetPool()
	ctx := context.Background()
	chunked := chunked(resources, 500)
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	// noop if no problems
	defer tx.Rollback(ctx)
	for _, chunk := range chunked {
		// how best to deal with chunked errors?
		// cancel entire transaction?
		err := batchDeleteResourcesFromResources(ctx, chunk, tx)
		if err != nil {
			return err
		}
	}
	err = tx.Commit(ctx)
	if err != nil {
		return errors.Wrap(err, "committing transaction")
	}
	return nil
}

func batchDeleteResourcesFromResources(ctx context.Context, resources []Identifiable, tx pgx.Tx) error {
	var ids = make([]string, 0)
	for _, resource := range resources {
		s := fmt.Sprintf("('%s', '%s')", resource.Identifier().Id, resource.Identifier().Type)
		ids = append(ids, s)
	}
	inClause := strings.Join(ids, ", ")

	sql := fmt.Sprintf(`DELETE from resources WHERE id, type IN (
		%s
	)`, inClause)

	_, err := tx.Exec(ctx, sql)

	if err != nil {
		return err
	}
	return nil
}

func BulkRemoveStagingDeletedFromResources(typeName string) error {
	deletes, err := RetrieveDeletedStaging(typeName)
	if err != nil {
		return err
	}
	err = BatchDeleteStagingFromResources(deletes...)
	if err != nil {
		return err
	}
	// TODO: then remove from staging?  or let caller ?
	// in theory could use to remove from solr, rdf etc...
	// but could also use notify
	// no errors - would catch later with 'orphan' check
	err = ClearStagingTypeDeletes(typeName)
	if err != nil {
		return err
	}
	return nil
}

func RemoveStagingDeletedFromResources(id string, typeName string) error {
	deleted, err := RetrieveSingleStagingDelete(id, typeName)
	if err != nil {
		return err
	}
	err = BatchDeleteStagingFromResources(deleted)
	if err != nil {
		return err
	}
	// TODO: then remove from staging?  or let caller ?
	// in theory could use to remove from solr, rdf etc...
	// but could also use notify
	// no errors - would catch later with 'orphan' check
	err = ClearDeletedFromStaging(id, typeName)
	if err != nil {
		return err
	}
	return nil
}

func BulkRemoveResources(items ...Identifiable) error {
	// should it go to trouble of adding to staging as delete
	// and then turn around and delete?
	err := BatchDeleteResourcesFromResources(items...)
	if err != nil {
		return err
	}
	return nil
}

func ResourceCount(typeName string) int {
	var count int
	ctx := context.Background()
	sql := `SELECT count(*) 
	FROM resources res
	WHERE type = $1`
	db := GetPool()
	row := db.QueryRow(ctx, sql, typeName)
	err := row.Scan(&count)
	if err != nil {
		log.Fatalf("error checking count %v", err)
	}
	return count
}

func GetMaxUpdatedAt(typeName string) time.Time {
	// NOTE: shouldn't be possible to be null, but
	// could be nothing of that typeName - therefore default to 1/1/2019
	var max time.Time
	ctx := context.Background()
	sql := `SELECT coalesce(max(updated_at), to_date('2019', 'YYYY'))
	FROM resources res
	WHERE type = $1`
	db := GetPool()
	row := db.QueryRow(ctx, sql, typeName)
	err := row.Scan(&max)
	// TODO: return error?
	if err != nil {
		log.Fatalf("error checking count %v", err)
	}
	return max
}

func RetrieveSingleResource(id string, typeName string) (Resource, error) {
	db := GetPool()
	ctx := context.Background()
	var found Resource

	findSQL := `SELECT id, type, data, created_at, updated_at
	  FROM resources
	  WHERE (id = $1 AND type = $2)`

	row := db.QueryRow(ctx, findSQL, id, typeName)

	err := row.Scan(&found.Id, &found.Type, &found.Data, &found.CreatedAt, &found.UpdatedAt)

	if err != nil {
		msg := fmt.Sprintf("ERROR: retrieiving single from resources: %s\n", err)
		return found, errors.New(msg)
	}
	return found, nil
}
