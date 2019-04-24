package staging_importer_test

import (
	"fmt"
	"log"
	"os"
	"testing"

	sj "gitlab.oit.duke.edu/scholars/staging_importer"
)

func setup() {
	// TODO: probably better way to do this
	database := sj.DatabaseInfo{
		Server:   "localhost",
		Database: "docker",
		Password: "docker",
		Port:     5432,
		User:     "docker",
	}
	config := sj.Config{
		Database: database,
	}

	// NOTE: this just makes connection
	err := sj.MakeConnection(config)
	if err != nil {
		log.Fatal("cannot connect to database")
	}

	if !sj.StagingTableExists() {
		fmt.Println("staging table not found")
		sj.MakeStagingSchema()
	}
	if !sj.ResourceTableExists() {
		fmt.Println("resources table not found")
		sj.MakeResourceSchema()
	}

	// empty everything out for tests
	sj.ClearAllStaging()
	sj.ClearAllResources()
}

func shutdown() {
	db := sj.GetConnection()
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

func (tp TestPerson) Uri() string {
	return fmt.Sprintf("https://scholars.duke.edu/individual/per%s", tp.Identifier())
}

func TestStagingIngest(t *testing.T) {
	person := &TestPerson{Id: "per0000001", Name: "Test"}
	typeName := "person"
	// 1. save
	sj.SaveStagingResource(person, typeName)
	// 2. retrieve
	exists := sj.StagingResourceExists("per0000001", "person")
	if exists != true {
		t.Error("did not save record")
	}
}

func TestStagingListValid(t *testing.T) {
	// clear out staging here
	sj.ClearAllStaging()

	// maybe interface with Id and TypeName ??
	person := &TestPerson{Id: "per0000001", Name: "Test"}
	typeName := "person"
	// 1. save
	sj.SaveStagingResource(person, typeName)
	// 2. retrieve
	alwaysOkay := func(json string) bool { return true }
	list, rejects := sj.FilterTypeStaging("person", alwaysOkay)

	t.Logf("list=%v\n", list)
	t.Logf("rejects=%v\n", rejects)
	// NOTE: not marked in db column yet
	if len(list) != 1 {
		t.Error("did not retrieve 1 and only 1 record")
	}
}

func TestStagingListInValid(t *testing.T) {
	// clear out staging here
	sj.ClearAllStaging()

	person := &TestPerson{Id: "per0000001", Name: "Test"}
	typeName := "person"
	// 1. save
	sj.SaveStagingResource(person, typeName)
	// 2. retrieve
	alwaysOkay := func(json string) bool { return false }
	list, rejects := sj.FilterTypeStaging("person", alwaysOkay)

	t.Logf("list=%v\n", list)
	t.Logf("rejects=%v\n", rejects)
	// NOTE: not marked in db column yet
	if len(rejects) != 1 {
		t.Error("did not retrieve 1 and only 1 record")
	}
}

func TestBulkAdd(t *testing.T) {
	// clear out staging here
	sj.ClearAllStaging()
	typeName := "person"

	person1 := TestPerson{Id: "per0000001", Name: "Test1"}
	person2 := TestPerson{Id: "per0000002", Name: "Test2"}

	people := []sj.Identifiable{person1, person2}
	err := sj.BulkAddStaging(typeName, people...)

	if err != nil {
		fmt.Println("could not save")
		t.Errorf("err=%v\n", err)
	}
	alwaysOkay := func(json string) bool { return true }
	list, rejects := sj.FilterTypeStaging("person", alwaysOkay)

	t.Logf("list=%v\n", list)
	t.Logf("rejects=%v\n", rejects)
	// NOTE: not marked in db column yet
	if len(list) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
}

func TestTypicalUsage(t *testing.T) {
	// clear out staging here
	sj.ClearAllStaging()
	typeName := "person"

	person1 := TestPerson{Id: "per0000001", Name: "Test1"}
	person2 := TestPerson{Id: "per0000002", Name: "Test2"}

	people := []sj.Identifiable{person1, person2}
	err := sj.StashTypeStaging(typeName, people...)

	if err != nil {
		fmt.Println("could not save")
		t.Errorf("err=%v\n", err)
	}

	all := sj.RetrieveTypeStaging(typeName)
	if len(all) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
	// what to check here?
	// should be 2 records is_valid = TRUE
	// should be 0 records is_valid = FALSE
	// should be 0 records is_valid is NULL
}

func TestBatchValid(t *testing.T) {
	// clear out staging here
	sj.ClearAllStaging()
	typeName := "person"

	person1 := TestPerson{Id: "per0000001", Name: "Test1"}
	person2 := TestPerson{Id: "per0000002", Name: "Test2"}

	people := []sj.Identifiable{person1, person2}
	err := sj.StashTypeStaging(typeName, people...)

	if err != nil {
		fmt.Println("could not save")
		t.Errorf("err=%v\n", err)
	}

	alwaysOkay := func(json string) bool { return true }
	valid, _ := sj.FilterTypeStaging(typeName, alwaysOkay)
	if len(valid) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
	sj.BatchMarkValidInStaging(valid)
	// should be two marked as 'valid' now
	list := sj.RetrieveValidStaging(typeName)
	if len(list) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
}

func TestBatchInValid(t *testing.T) {
	// clear out staging here
	sj.ClearAllStaging()
	typeName := "person"

	person1 := TestPerson{Id: "per0000001", Name: "Test1"}
	person2 := TestPerson{Id: "per0000002", Name: "Test2"}

	people := []sj.Identifiable{person1, person2}
	err := sj.StashTypeStaging(typeName, people...)

	if err != nil {
		fmt.Println("could not save")
		t.Errorf("err=%v\n", err)
	}

	alwaysBad := func(json string) bool { return false }
	_, rejects := sj.FilterTypeStaging(typeName, alwaysBad)
	if len(rejects) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}

	sj.BatchMarkInvalidInStaging(rejects)
	// should be two marked as 'valid' now
	list := sj.RetrieveInvalidStaging(typeName)
	if len(list) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
}
