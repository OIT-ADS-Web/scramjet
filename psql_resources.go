package staging_importer

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	//"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
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

// NOTE: this means all things have to have id field
type GenericResource struct {
	Id string `json:"id"`
}

// just a stub so we can read from resources table
// and get the id match back to staging (for deletes)
func (g GenericResource) Identifier() string {
	return g.Id
}

func (res Resource) Identifier() string {
	identified := GenericResource{}
	if res.DataB.Status == pgtype.Present {
		b := res.DataB.Bytes
		err := json.Unmarshal(b, &identified)
		if err != nil {
			fmt.Printf("error unmarshalling json %#v\n", res)
		}
	}
	return identified.Identifier()
}

//func DeriveUri(u UriAddressable) string { return u.URI() }

// Resources ...

// TODO: could just send in date - leave it up to library user
// to determine how it's figured out
func RetrieveTypeResources(typeName string) (error, []Resource) {
	db := GetPool()
	resources := []Resource{}
	ctx := context.Background()
	var err error
	sql := `SELECT uri, type, hash, data, data_b
		FROM resources 
		WHERE type =  $1
		`
	rows, _ := db.Query(ctx, sql, typeName)

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

	if err != nil {
		return err, nil
	}
	return nil, resources
}

func RetrieveTypeResourcesLimited(typeName string, limit int) ([]Resource, error) {
	db := GetPool()
	resources := []Resource{}
	ctx := context.Background()
	var err error
	sql := `SELECT uri, type, hash, data, data_b
		FROM resources 
		WHERE type =  $1
		LIMIT $2
		`
	rows, _ := db.Query(ctx, sql, typeName, limit)

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

// FIXME: would like a way to do multiple at a time - some kind of upsert?
func SaveResource(obj UriAddressable, typeName string) (err error) {
	ctx := context.Background()
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

	if err != nil {
		return err
	}
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

	row := db.QueryRow(ctx, findSQL, obj.Uri(), typeName)
	notFoundError := row.Scan(&found.Uri, &found.Type)

	tx, err := db.Begin(ctx)

	// either insert or update
	if notFoundError != nil {
		// TODO: created_at, updated_at
		sql := `INSERT INTO resources (uri, type, hash, data, data_b) 
	      VALUES ($1, $2, $3, $4, $5)`
		_, err := tx.Exec(ctx, sql, res.Uri, res.Type, res.Hash, &res.Data, &res.DataB)

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
			_, err := tx.Exec(ctx, sql, res.Uri, res.Type, res.Hash, &res.Data, &res.DataB)

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
	// FIXME: not sure this is right
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
        uri text NOT NULL,
        type text NOT NULL,
        hash text NOT NULL,
        data json NOT NULL,
        data_b jsonb NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),
		PRIMARY KEY(uri, type),
		CONSTRAINT uniq_uri_hash UNIQUE (uri, type, hash)
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

// TODO: should probably return error -  not have os.Exit

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

func ClearAllResources() (err error) {
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

// TODO: should probably return error -  not have os.Exit
func ClearResourceType(typeName string) (err error) {
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

func uniqueUri(idSlice []UriAddressable) []UriAddressable {
	keys := make(map[string]bool)
	list := []UriAddressable{}
	for _, entry := range idSlice {
		if _, value := keys[entry.Uri()]; !value {
			keys[entry.Uri()] = true
			list = append(list, entry)
		}
	}
	return list
}

// add many at a time (upsert) - don't need a makeUri function
// (that's what's different from BulkAddResourcesStagingResource)
// FIXME: a lot of boilerplate code exactly the same
func BulkAddResources(typeName string, items ...UriAddressable) error {
	var resources = make([]Resource, 0)
	var err error
	// NOTE: not sure if these are necessary
	list := uniqueUri(items)

	for _, item := range list {
		str, err := json.Marshal(item)
		if err != nil {
			log.Fatalln(err)
		}

		hash := makeHash(string(str))

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

		res := &Resource{Uri: item.Uri(),
			Type:  typeName,
			Hash:  hash,
			Data:  data,
			DataB: dataB}
		resources = append(resources, *res)
	}

	db := GetPool()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		log.Printf("error starting transaction =%v\n", err)
	}

	// supposedly no-op if everything okay
	defer tx.Rollback(ctx)

	tmpSql := `CREATE TEMPORARY TABLE resource_data_tmp
	  (uri text NOT NULL, type text NOT NULL, hash text NOT NULL,
		data json NOT NULL, data_b jsonb NOT NULL,
		PRIMARY KEY(uri, type)
	  )
	  ON COMMIT DROP
	`
	_, err = tx.Exec(ctx, tmpSql)

	if err != nil {
		log.Printf("error=%s\n", err)
		return errors.Wrap(err, "creating temporary table")
	}

	// NOTE: don't commit yet (see ON COMMIT DROP)
	inputRows := [][]interface{}{}
	for _, res := range resources {
		x := []byte{}
		readError := res.Data.AssignTo(&x)

		if readError != nil {
			// do something else here, mark error somewhere?
			fmt.Printf("skipping %s:%s\n", res.Uri, readError)
			continue
		}
		inputRows = append(inputRows, []interface{}{res.Uri,
			res.Type,
			res.Hash,
			x,
			x})
	}

	_, err = tx.CopyFrom(ctx, pgx.Identifier{"resource_data_tmp"},
		[]string{"uri", "type", "hash", "data", "data_b"},
		pgx.CopyFromRows(inputRows))

	if err != nil {
		fmt.Printf("error=%s\n", err)
		return err
	}

	sqlUpsert := `INSERT INTO resources (uri, type, hash, data, data_b)
	  SELECT uri, type, hash, data, data_b
	  FROM resource_data_tmp
		ON CONFLICT (uri, type) DO UPDATE SET data = EXCLUDED.data,
		   data_b = EXCLUDED.data_b, hash = EXCLUDED.hash
	`
	_, err = tx.Exec(ctx, sqlUpsert)
	if err != nil {
		log.Printf("error=%s\n", err)
		return errors.Wrap(err, "move from temporary to real table")
	}

	// now flag as 'updated' if hash changed (had to split this up into two sql calls)
	// TODO: in theory uri should be primary key and enough to identify
	sqlUpdates := `UPDATE resources set updated_at = NOW()
	where (uri,type) in (
		select rdt.uri, rdt.type from resource_data_tmp rdt
		join resources r on (r.uri = rdt.uri and r.type = rdt.type)
		where r.hash != rdt.hash
	)`

	_, err = tx.Exec(ctx, sqlUpdates)
	if err != nil {
		log.Printf("error=%s\n", err)
		return errors.Wrap(err, "move from temporary to real table")
	}

	err = tx.Commit(ctx)
	if err != nil {
		return errors.Wrap(err, "commit transaction")
	}
	return nil
}

func BulkMoveStagingToResources(typeName string, uriMaker UriFunc, items ...StagingResource) error {
	var resources = make([]Resource, 0)
	var err error
	ctx := context.Background()

	for _, item := range items {
		str := item.Data
		// need way to get URI (given a staging resource)
		uri := uriMaker(item)

		hash := makeHash(string(str))

		var data pgtype.JSON
		var dataB pgtype.JSONB
		err = data.Set(str)

		if err != nil {
			return err
		}

		// same value - is that a problem?
		err = dataB.Set(str)

		if err != nil {
			return err
		}

		res := &Resource{Uri: uri,
			Type:  typeName,
			Hash:  hash,
			Data:  data,
			DataB: dataB}
		resources = append(resources, *res)
	}

	db := GetPool()

	tx, err := db.Begin(ctx)
	if err != nil {
		log.Printf("error starting transaction =%v\n", err)
	}

	// supposedly no-op if everything okay
	defer tx.Rollback(ctx)

	tmpSql := `CREATE TEMPORARY TABLE resource_data_tmp
	  (uri text NOT NULL, type text NOT NULL, hash text NOT NULL,
		data json NOT NULL, data_b jsonb NOT NULL,
		PRIMARY KEY(uri, type)
	  )
	  ON COMMIT DROP
	`
	_, err = tx.Exec(ctx, tmpSql)

	if err != nil {
		log.Printf("error=%s\n", err)
		return errors.Wrap(err, "creating temporary table")
	}

	// NOTE: don't commit yet (see ON COMMIT DROP)
	inputRows := [][]interface{}{}
	for _, res := range resources {
		x := []byte{}
		readError := res.Data.AssignTo(&x)
		if readError != nil {
			// do something else here, mark error somewhere?
			fmt.Printf("skipping %s:%s\n", res.Uri, readError)
			continue
		}
		y := []byte{}
		readError = res.DataB.AssignTo(&y)

		if readError != nil {
			// do something else here, mark error somewhere?
			fmt.Printf("skipping %s:%s\n", res.Uri, readError)
			continue
		}
		inputRows = append(inputRows, []interface{}{res.Uri,
			res.Type,
			res.Hash,
			x,
			y})
	}

	_, err = tx.CopyFrom(ctx, pgx.Identifier{"resource_data_tmp"},
		[]string{"uri", "type", "hash", "data", "data_b"},
		pgx.CopyFromRows(inputRows))

	if err != nil {
		fmt.Printf("error=%s\n", err)
		return err
	}

	// updated_at - should probably be timezone aware ...
	// ON CONFLICT (uri, type) where hash != EXCLUDED.hash
	sqlUpsert := `INSERT INTO resources (uri, type, hash, data, data_b)
	  SELECT uri, type, hash, data, data_b 
	  FROM resource_data_tmp
		ON CONFLICT (uri, type) DO UPDATE SET data = EXCLUDED.data, 
		   data_b = EXCLUDED.data_b, hash = EXCLUDED.hash
	`
	_, err = tx.Exec(ctx, sqlUpsert)
	if err != nil {
		log.Printf("error=%s\n", err)
		return errors.Wrap(err, "move from temporary to real table")
	}

	// now flag as 'updated' if hash changed (had to split this up into two sql calls)
	// TODO: in theory uri should be primary key and enough to identify
	sqlUpdates := `UPDATE resources set updated_at = NOW()
	where (uri,type) in (
		select rdt.uri, rdt.type from resource_data_tmp rdt
		join resources r on (r.uri = rdt.uri and r.type = rdt.type)
		where r.hash != rdt.hash
	)`

	_, err = tx.Exec(ctx, sqlUpdates)
	if err != nil {
		log.Printf("error=%s\n", err)
		return errors.Wrap(err, "move from temporary to real table")
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Printf("error=%s\n", err)
		return errors.Wrap(err, "commit transaction")
	}
	err = ClearStagingTypeValid(typeName)
	if err != nil {
		log.Printf("error=%s\n", err)
		return errors.Wrap(err, "clearing staging table")
	}
	return nil
}

//https://stackoverflow.com/questions/35179656/slice-chunking-in-go
func chunkedResources(resources []UriAddressable, chunkSize int) [][]UriAddressable {
	var divided [][]UriAddressable

	for i := 0; i < len(resources); i += chunkSize {
		end := i + chunkSize

		if end > len(resources) {
			end = len(resources)
		}

		divided = append(divided, resources[i:end])
	}
	return divided
}

func BatchDeleteFromResources(resources []UriAddressable) (err error) {
	db := GetPool()
	ctx := context.Background()
	chunked := chunkedResources(resources, 500)
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	// noop if no problems
	defer tx.Rollback(ctx)
	for _, chunk := range chunked {
		// how best to deal with chunked errors?
		// cancel entire transaction?
		err := batchDeleteFromResources(ctx, chunk, tx)
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

// get typename ??
func BatchDeleteFromResources2(resources []StagingResource) (err error) {
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
		// how best to deal with chunked errors?
		// cancel entire transaction?
		err := batchDeleteFromResources2(ctx, chunk, tx)
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

// TODO: lower, uppercase matching names maybe confusing
func batchDeleteFromResources(ctx context.Context, resources []UriAddressable, tx pgx.Tx) (err error) {
	// NOTE: this would need to only do 500-750 (or so) at a time
	// because of SQL IN clause limit of 1000
	//db := GetPool()
	//ctx := context.Background()
	// TODO: better ways to do this
	var uris = make([]string, 0)

	for _, resource := range resources {
		s := fmt.Sprintf("'%s'", resource.Uri())
		uris = append(uris, s)
	}

	inClause := strings.Join(uris, ", ")

	sql := fmt.Sprintf(`DELETE from resources WHERE uri IN (
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

// how to enusure staging-resource IS identifiable
func batchDeleteFromResources2(ctx context.Context, resources []StagingResource, tx pgx.Tx) (err error) {
	//ctx := context.Background()
	// TODO: better ways to do this
	//var uris = make([]string, 0)

	var clauses = make([]string, 0)
	for _, resource := range resources {
		//s := fmt.Sprintf("'%s'", resource.Identifier())
		s := fmt.Sprintf("('%s', '%s')", resource.Id, resource.Type)
		//uris = append(uris, s)
		clauses = append(clauses, s)
	}

	// what about including 'type'?
	//inClause := strings.Join(uris, ", ")

	inClause := strings.Join(clauses, ", ")

	//sql := fmt.Sprintf(`DELETE from resources WHERE data_b->>'id' IN (
	//	  %s
	//)`, inClause)

	// not crazy about this ...
	sql := fmt.Sprintf(`DELETE from resources WHERE (data_b->>'id', type) IN (
		%s
	)`, inClause)

	_, err = tx.Exec(ctx, sql)

	if err != nil {
		return err
	}
	return nil
}

// just a stub - so I can match staging to resource table
type UriOnly struct {
	Fn  UriFunc
	Res StagingResource
}

func (uri UriOnly) Uri() string {
	return uri.Fn(uri.Res)
}

func BulkRemoveDeletedResources(typeName string, uriMaker UriFunc) (err error) {
	deletes := RetrieveDeletedStaging(typeName)
	toRemove := []UriAddressable{}
	for _, res := range deletes {
		stub := UriOnly{Res: res, Fn: uriMaker}
		toRemove = append(toRemove, stub)
	}
	err = BatchDeleteFromResources(toRemove)
	// err = BatchDeleteFromResources2(deletes)
	if err != nil {
		return err
	}
	// then remove from staging?  or let caller ?
	// in theory could use to remove from solr, rdf etc...
	// but could also use notify
	// no errors - would catch later with 'orphan' check
	err = ClearStagingTypeDeletes(typeName)
	if err != nil {
		return err
	}
	return nil
}

func BulkRemoveDeletedResources2(typeName string) (err error) {
	deletes := RetrieveDeletedStaging(typeName)
	//toRemove := []UriAddressable{}
	//for _, res := range deletes {
	//	stub := UriOnly{Res: res, Fn: uriMaker}
	//	toRemove = append(toRemove, stub)
	//}
	//err = BatchDeleteFromResources(toRemove)
	err = BatchDeleteFromResources2(deletes)
	if err != nil {
		return err
	}
	// then remove from staging?  or let caller ?
	// in theory could use to remove from solr, rdf etc...
	// but could also use notify
	// no errors - would catch later with 'orphan' check
	err = ClearStagingTypeDeletes(typeName)
	if err != nil {
		return err
	}
	return nil
}

// TODO: maybe a more intuitive name - especially if this is the only
// way to delete - like BulkDelete()
func BulkDeleteResources(typeName string, uriMaker UriFunc, items ...Identifiable) error {
	err := BulkAddStagingForDelete(typeName, items...)
	if err != nil {
		return err
	}
	err = BulkRemoveDeletedResources(typeName, uriMaker)
	if err != nil {
		return err
	}
	return nil
}

// TODO: different from above, but name overly similar
func BulkRemoveResources(items ...UriAddressable) error {
	// should it go to trouble of adding to staging as delete
	// and then turn around and delete?  but then need
	// the id-matcher (uriMaker)
	err := BatchDeleteFromResources(items)
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
