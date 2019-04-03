package psql_test

import (
	"fmt"
	"log"
	"os"
	"testing"

	"gitlab.oit.duke.edu/scholars/staging_importer/config"
	"gitlab.oit.duke.edu/scholars/staging_importer/psql"
)

func setup() {
	// TODO: probably better way to do this
	database := config.Database{
		Server:   "localhost",
		Database: "docker",
		Password: "docker",
		Port:     5432,
		User:     "docker",
	}
	config := config.Config{
		Database: database,
	}

	// NOTE: this just makes connection
	err := psql.MakeConnection(config)
	if err != nil {
		log.Fatal("cannot connect to database")
	}

	if !psql.StagingTableExists() {
		fmt.Println("staging table not found")
		psql.MakeStagingSchema()
	}
	if !psql.ResourceTableExists() {
		fmt.Println("resources table not found")
		psql.MakeResourceSchema()
	}

	psql.ClearAllStaging()
	psql.ClearAllResources()
}

func shutdown() {
	db := psql.GetConnection()
	db.Close()
}
func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func TestStagingIngest(t *testing.T) {
	type TestPerson struct {
		Name string
	}
	// maybe interface with Id and TypeName ??
	person := &TestPerson{Name: "Test"}
	id := "per0000001"
	typeName := "person"
	// 1. save
	psql.SaveStagingResource(person, id, typeName)
	// 2. retrieve
	exists := psql.StagingResourceExists("per0000001", "person")
	if exists != true {
		t.Error("did not save record")
	}
}

func TestStagingListValid(t *testing.T) {
	psql.ClearAllStaging()
	type TestPerson struct {
		Name string
	}
	// maybe interface with Id and TypeName ??
	person := &TestPerson{Name: "Test"}
	id := "per0000001"
	typeName := "person"
	// 1. save
	psql.SaveStagingResource(person, id, typeName)
	// 2. retrieve
	alwaysOkay := func(json string) bool { return true }
	list, rejects := psql.FilterStagingList("person", alwaysOkay)

	t.Logf("list=%v\n", list)
	t.Logf("rejects=%v\n", rejects)
	if len(list) != 1 {
		t.Error("did not retrieve 1 and only 1 record")
	}
}

func TestStagingListInValid(t *testing.T) {
	psql.ClearAllStaging()
	type TestPerson struct {
		Name string
	}
	// maybe interface with Id and TypeName ??
	person := &TestPerson{Name: "Test"}
	id := "per0000001"
	typeName := "person"
	// 1. save
	psql.SaveStagingResource(person, id, typeName)
	// 2. retrieve
	alwaysOkay := func(json string) bool { return false }
	list, rejects := psql.FilterStagingList("person", alwaysOkay)

	t.Logf("list=%v\n", list)
	t.Logf("rejects=%v\n", rejects)
	// NOTE: not marked yet
	if len(rejects) != 1 {
		t.Error("did not retrieve 1 and only 1 record")
	}
}

// TODO:
// MarkInvalidInStaging()
