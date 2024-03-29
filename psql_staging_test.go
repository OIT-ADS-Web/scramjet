package scramjet_test

import (
	"fmt"
	"os"
	"testing"

	sj "github.com/OIT-ADS-Web/scramjet"
)

func setup() {
	// TODO: probably better way to do this
	// NOTE: changed to docker setup user (couldn't get 'docker' user recognized)
	database := sj.DatabaseInfo{
		Server:         "localhost",
		Database:       "json_data",
		Password:       "json_data",
		Port:           5433,
		User:           "json_data",
		MaxConnections: 1,
		AcquireTimeout: 30,
		Application:    "test",
	}
	config := sj.Config{
		Database: database,
	}
	sj.Configure(config)
	sj.ClearAllStaging()
	sj.ClearAllResources()
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

type TestPersonExtended struct {
	Id         string `json:"id"`
	Name       string `json:"name"`
	ExternalId string `json:"externalId"`
}

type TestPublication struct {
	Id    string `json:"id"`
	Title string `json:"title"`
}

type TestAuthorship struct {
	Id            string `json:"id"`
	PublicationId string `json:"publicationId"`
	PersonId      string `json:"personId"`
}

func TestStagingIngest(t *testing.T) {
	sj.ClearAllStaging()
	person := &TestPerson{Id: "per0000001", Name: "Test"}
	typeName := "person"
	pass1 := sj.Packet{Id: sj.Identifier{Id: person.Id, Type: typeName}, Obj: person}
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
	pass1 := sj.Packet{Id: sj.Identifier{Id: person.Id, Type: typeName}, Obj: person}
	// 1. save
	err := sj.SaveStagingResource(pass1)
	if err != nil {
		t.Error("could not save record")
	}

	// 2. retrieve
	alwaysOkay := func(json string) bool { return true }
	list, rejects, err := sj.FilterTypeStaging("person", alwaysOkay)

	if err != nil {
		t.Error("could not validate list")
	}
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
	pass1 := sj.Packet{Id: sj.Identifier{Id: person.Id, Type: typeName}, Obj: person}
	// 1. save
	err := sj.SaveStagingResource(pass1)
	if err != nil {
		t.Error("could not save record")
	}
	// 2. retrieve
	alwaysOkay := func(json string) bool { return false }
	list, rejects, err := sj.FilterTypeStaging("person", alwaysOkay)
	if err != nil {
		t.Error("could not validate list")
	}
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
	pass1 := sj.Packet{Id: sj.Identifier{Id: person1.Id, Type: typeName}, Obj: person1}
	pass2 := sj.Packet{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}
	people := []sj.Storeable{pass1, pass2}

	err := sj.BulkAddStaging(people...)

	if err != nil {
		fmt.Println("could not save")
		t.Errorf("err=%v\n", err)
	}
	alwaysOkay := func(json string) bool { return true }
	list, rejects, err := sj.FilterTypeStaging("person", alwaysOkay)

	if err != nil {
		t.Error("could not validate list")
	}
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
	pass1 := sj.Packet{Id: sj.Identifier{Id: person1.Id, Type: typeName}, Obj: person1}
	pass2 := sj.Packet{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}
	people := []sj.Storeable{pass1, pass2}

	err := sj.StashStaging(people...)

	if err != nil {
		fmt.Println("could not save")
		t.Errorf("err=%v\n", err)
	}

	all, err := sj.RetrieveTypeStaging(typeName)
	if err != nil {
		t.Errorf("coud not get list from staging;err=%v\n", err)
	}

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
	pass1 := sj.Packet{Id: sj.Identifier{Id: person1.Id, Type: typeName}, Obj: person1}
	pass2 := sj.Packet{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}
	people := []sj.Storeable{pass1, pass2}

	err := sj.StashStaging(people...)

	if err != nil {
		fmt.Println("could not save")
		t.Errorf("err=%v\n", err)
	}

	alwaysOkay := func(json string) bool { return true }
	valid, _, _ := sj.FilterTypeStaging(typeName, alwaysOkay)
	if len(valid) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
	err = sj.BatchMarkValidInStaging(valid)
	if err != nil {
		t.Error("error marking records valid")
	}
	// should be two marked as 'valid' now
	list, err := sj.RetrieveValidStaging(typeName)
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
	pass1 := sj.Packet{Id: sj.Identifier{Id: person1.Id, Type: typeName}, Obj: person1}
	pass2 := sj.Packet{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}
	people := []sj.Storeable{pass1, pass2}

	err := sj.StashStaging(people...)

	if err != nil {
		fmt.Println("could not save")
		t.Errorf("err=%v\n", err)
	}

	alwaysBad := func(json string) bool { return false }
	_, rejects, _ := sj.FilterTypeStaging(typeName, alwaysBad)
	if len(rejects) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}

	err = sj.BatchMarkInvalidInStaging(rejects)
	// should be two marked as 'valid' now
	list, err := sj.RetrieveInvalidStaging(typeName)
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
	pass1 := sj.Packet{Id: sj.Identifier{Id: person1.Id, Type: typeName}, Obj: person1}
	pass2 := sj.Packet{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}
	people := []sj.Storeable{pass1, pass2}

	err := sj.StashStaging(people...)

	if err != nil {
		fmt.Println("could not save")
		t.Errorf("err=%v\n", err)
	}
	alwaysOkay := func(json string) bool { return true }
	valid, _, err := sj.FilterTypeStaging("person", alwaysOkay)
	// should be no rejects

	// NOTE: just immediately marking for delete
	err = sj.BulkAddStagingForDelete(valid...)

	if err != nil {
		fmt.Println("could not mark for delete")
		t.Errorf("err=%v\n", err)
	}
	list, _ := sj.RetrieveDeletedStaging(typeName)
	if len(list) != 2 {
		t.Error("did not retrieve 2 and only 2 record (for delete)")
	}
}
