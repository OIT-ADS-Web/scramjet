package psql_test

import (
	"fmt"
	"log"
	"os"
	"testing"

	si "gitlab.oit.duke.edu/scholars/staging_importer"
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

	// empty everything out for tests
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

type TestPerson struct {
	Id   string
	Name string
}

func (tp TestPerson) Identifier() string {
	return tp.Id
}

func TestStagingIngest(t *testing.T) {
	person := &TestPerson{Id: "per0000001", Name: "Test"}
	typeName := "person"
	// 1. save
	psql.SaveStagingResource(person, typeName)
	// 2. retrieve
	exists := psql.StagingResourceExists("per0000001", "person")
	if exists != true {
		t.Error("did not save record")
	}
}

func TestStagingListValid(t *testing.T) {
	// clear out staging here
	psql.ClearAllStaging()

	// maybe interface with Id and TypeName ??
	person := &TestPerson{Id: "per0000001", Name: "Test"}
	typeName := "person"
	// 1. save
	psql.SaveStagingResource(person, typeName)
	// 2. retrieve
	alwaysOkay := func(json string) bool { return true }
	list, rejects := psql.FilterTypeStaging("person", alwaysOkay)

	t.Logf("list=%v\n", list)
	t.Logf("rejects=%v\n", rejects)
	// NOTE: not marked in db column yet
	if len(list) != 1 {
		t.Error("did not retrieve 1 and only 1 record")
	}
}

func TestStagingListInValid(t *testing.T) {
	// clear out staging here
	psql.ClearAllStaging()

	person := &TestPerson{Id: "per0000001", Name: "Test"}
	typeName := "person"
	// 1. save
	psql.SaveStagingResource(person, typeName)
	// 2. retrieve
	alwaysOkay := func(json string) bool { return false }
	list, rejects := psql.FilterTypeStaging("person", alwaysOkay)

	t.Logf("list=%v\n", list)
	t.Logf("rejects=%v\n", rejects)
	// NOTE: not marked in db column yet
	if len(rejects) != 1 {
		t.Error("did not retrieve 1 and only 1 record")
	}
}

func TestBulkAdd(t *testing.T) {
	// clear out staging here
	psql.ClearAllStaging()
	typeName := "person"

	person1 := TestPerson{Id: "per0000001", Name: "Test1"}
	person2 := TestPerson{Id: "per0000002", Name: "Test2"}

	people := []si.Identifiable{person1, person2}
	err := psql.BulkAddStaging(typeName, people...)

	if err != nil {
		fmt.Println("could not save")
		t.Errorf("err=%v\n", err)
	}
	alwaysOkay := func(json string) bool { return true }
	list, rejects := psql.FilterTypeStaging("person", alwaysOkay)

	t.Logf("list=%v\n", list)
	t.Logf("rejects=%v\n", rejects)
	// NOTE: not marked in db column yet
	if len(list) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
}
