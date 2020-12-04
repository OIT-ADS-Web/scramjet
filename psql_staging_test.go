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
		Server:         "localhost",
		Database:       "docker",
		Password:       "docker",
		Port:           5433,
		User:           "docker",
		MaxConnections: 1,
		AcquireTimeout: 30,
	}
	config := sj.Config{
		Database: database,
	}

	err := sj.MakeConnectionPool(config)

	if err != nil {
		log.Fatal("cannot connect to database")
	}

	err = sj.DropStaging()
	if err != nil {
		log.Fatalf("cannot delete staging database %s\n", err)
	}

	if !sj.StagingTableExists() {
		fmt.Println("staging table not found")
		sj.MakeStagingSchema()
	}
	err = sj.DropResources()
	if err != nil {
		log.Fatal("cannot delete resources database")
	}
	if !sj.ResourceTableExists() {
		fmt.Println("resources table not found")
		sj.MakeResourceSchema()
	}
}

func shutdown() {
	db := sj.GetPool()
	db.Close()
}
func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}

// NOTE: for delete to work, json needs to be 'id' lowercase
type TestPerson struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

/*
func (tp TestPerson) Identifier() string {
	return tp.Id
}

func (tp TestPerson) Grouping() string {
	return "person"
}
*/

func TestStagingIngest(t *testing.T) {
	sj.ClearAllStaging()
	person := &TestPerson{Id: "per0000001", Name: "Test"}
	typeName := "person"
	pass1 := sj.Passenger{Id: sj.Identifier{Id: person.Id, Type: typeName}, Obj: person}
	// 1. save
	err := sj.SaveStagingResource(pass1)
	if err != nil {
		t.Error("error saving record")
	}
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
	pass1 := sj.Passenger{Id: sj.Identifier{Id: person.Id, Type: typeName}, Obj: person}
	// 1. save
	err := sj.SaveStagingResource(pass1)
	if err != nil {
		t.Error("could not save record")
	}

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
	pass1 := sj.Passenger{Id: sj.Identifier{Id: person.Id, Type: typeName}, Obj: person}
	// 1. save
	err := sj.SaveStagingResource(pass1)
	if err != nil {
		t.Error("could not save record")
	}
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
	pass1 := sj.Passenger{Id: sj.Identifier{Id: person1.Id, Type: typeName}, Obj: person1}
	pass2 := sj.Passenger{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}
	people := []sj.Identifiable{pass1, pass2}

	err := sj.BulkAddStaging(people...)

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
	pass1 := sj.Passenger{Id: sj.Identifier{Id: person1.Id, Type: typeName}, Obj: person1}
	pass2 := sj.Passenger{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}
	people := []sj.Identifiable{pass1, pass2}

	err := sj.StashStaging(people...)

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
	pass1 := sj.Passenger{Id: sj.Identifier{Id: person1.Id, Type: typeName}, Obj: person1}
	pass2 := sj.Passenger{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}
	people := []sj.Identifiable{pass1, pass2}

	err := sj.StashStaging(people...)

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
	pass1 := sj.Passenger{Id: sj.Identifier{Id: person1.Id, Type: typeName}, Obj: person1}
	pass2 := sj.Passenger{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}
	people := []sj.Identifiable{pass1, pass2}

	err := sj.StashStaging(people...)

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

func TestBatchMarkDelete(t *testing.T) {
	// clear out staging here
	sj.ClearAllStaging()
	typeName := "person"

	person1 := TestPerson{Id: "per0000001", Name: "Test1"}
	person2 := TestPerson{Id: "per0000002", Name: "Test2"}
	pass1 := sj.Passenger{Id: sj.Identifier{Id: person1.Id, Type: typeName}, Obj: person1}
	pass2 := sj.Passenger{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}
	people := []sj.Identifiable{pass1, pass2}

	err := sj.StashStaging(people...)

	if err != nil {
		fmt.Println("could not save")
		t.Errorf("err=%v\n", err)
	}
	alwaysOkay := func(json string) bool { return true }
	valid, _ := sj.FilterTypeStaging("person", alwaysOkay)
	// should be no rejects

	// NOTE: just immediately marking for delete
	err = sj.BatchMarkDeleteInStaging(valid)
	if err != nil {
		fmt.Println("could not mark for delete")
		t.Errorf("err=%v\n", err)
	}
	list := sj.RetrieveDeletedStaging(typeName)
	if len(list) != 2 {
		t.Error("did not retrieve 2 and only 2 record (for delete)")
	}
}
