package scramjet_test

import (
	"testing"

	sj "github.com/OIT-ADS-Web/scramjet"
)

type IntakePerson struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

type IntakeNewsfeed struct {
	Id       string `json:"id"`
	PersonId string `json:"personId"`
	Title    string `json:"name"`
}

func TestFullIntake(t *testing.T) {
	sj.ClearAllStaging()
	sj.ClearAllResources()
	typeName := "person"

	// typically this is how a list might be created
	dbList := func() []IntakePerson {
		person1 := IntakePerson{Id: "per0000001", Name: "Test1"}
		person2 := IntakePerson{Id: "per0000002", Name: "Test2"}
		return []IntakePerson{person1, person2}
	}

	listMaker := func(i int) ([]sj.Storeable, error) {
		var people []sj.Storeable
		for _, person := range dbList() {
			pass := sj.MakePacket(person.Id, typeName, person)
			people = append(people, pass)
		}
		return people, nil
	}

	// try simple, non-filter version (all of a type)
	alwaysOkay := func(json string) bool { return true }
	// NOTE: count is entire list - in case list has to be chunked
	intake := sj.IntakeConfig{TypeName: typeName, Count: 2, ChunkSize: 1, ListMaker: listMaker}
	move := sj.TrajectConfig{TypeName: typeName, Validator: alwaysOkay}

	// typically this would call source datasource for all ids of 'type'
	// comparing against resources ids of 'type'
	ids := func() ([]string, error) {
		var ids []string
		for _, person := range dbList() {
			ids = append(ids, person.Id)
		}
		return ids, nil
	}
	outake := sj.OutakeConfig{TypeName: typeName, ListMaker: ids}

	// main function to do all 3 in one sequence
	err := sj.Scramjet(intake, move, outake)

	if err != nil {
		t.Errorf("err=%v\n", err)
	}

	count := sj.ResourceCount(typeName)
	if count != 2 {
		t.Errorf("after import should be 2 records - not :%d\n", count)
	}
}

func TestUpdatesIntake(t *testing.T) {
	sj.ClearAllStaging()
	sj.ClearAllResources()
	typeName := "person"

	// typically this is how a list might be created
	dbList := func() []IntakePerson {
		person1 := IntakePerson{Id: "per0000001", Name: "Test1"}
		person2 := IntakePerson{Id: "per0000002", Name: "Test2"}
		return []IntakePerson{person1, person2}
	}

	listMaker := func(i int) ([]sj.Storeable, error) {
		var people []sj.Storeable
		for _, person := range dbList() {
			pass := sj.MakePacket(person.Id, typeName, person)
			people = append(people, pass)
		}
		return people, nil
	}

	// try simple, non-filter version (all of a type)
	alwaysOkay := func(json string) bool { return true }
	// NOTE: count is entire list - in case list has to be chunked
	intake := sj.IntakeConfig{TypeName: typeName, Count: 2, ChunkSize: 1, ListMaker: listMaker}
	move := sj.TrajectConfig{TypeName: typeName, Validator: alwaysOkay}

	err := sj.ScramjetIntake(intake, move)

	if err != nil {
		t.Errorf("err=%v\n", err)
	}

	count := sj.ResourceCount(typeName)
	if count != 2 {
		t.Errorf("after import should be 2 records - not :%d\n", count)
	}
}

func TestEjectIntake(t *testing.T) {
	sj.ClearAllStaging()
	sj.ClearAllResources()
	typeName := "person"

	// typically this is how a list might be created
	dbList := func() []IntakePerson {
		person1 := IntakePerson{Id: "per0000001", Name: "Test1"}
		person2 := IntakePerson{Id: "per0000002", Name: "Test2"}
		return []IntakePerson{person1, person2}
	}

	listMaker := func(i int) ([]sj.Storeable, error) {
		var people []sj.Storeable
		for _, person := range dbList() {
			pass := sj.MakePacket(person.Id, typeName, person)
			people = append(people, pass)
		}
		return people, nil
	}

	// try simple, non-filter version (all of a type)
	alwaysOkay := func(json string) bool { return true }
	// NOTE: count is entire list - in case list has to be chunked
	intake := sj.IntakeConfig{TypeName: typeName, Count: 2, ChunkSize: 1, ListMaker: listMaker}
	move := sj.TrajectConfig{TypeName: typeName, Validator: alwaysOkay}

	// only import
	err := sj.ScramjetIntake(intake, move)

	if err != nil {
		t.Errorf("err=%v\n", err)
	}

	// they should be added now (see previous test)
	// now mark 1 for delete
	// this is returning all valid records (so per000002 is the delete)
	ids := func() ([]string, error) {
		return []string{"per0000001"}, nil
	}
	outake := sj.OutakeConfig{TypeName: typeName, ListMaker: ids}

	// then only removing
	err = sj.ScramjetOutake(outake)
	if err != nil {
		t.Errorf("err=%v\n", err)
	}

	count := sj.ResourceCount(typeName)
	if count != 1 {
		t.Errorf("after import and delete should be 1 record - not :%d\n", count)
	}
}

func TestFilteredIntake(t *testing.T) {
	sj.ClearAllStaging()
	sj.ClearAllResources()
	typeName := "person"

	// typically this is how a list might be created
	dbList := func() []IntakePerson {
		person1 := IntakePerson{Id: "per0000001", Name: "Test1"}
		person2 := IntakePerson{Id: "per0000002", Name: "Test2"}
		return []IntakePerson{person1, person2}
	}

	listMaker := func(i int) ([]sj.Storeable, error) {
		var people []sj.Storeable
		for _, person := range dbList() {
			pass := sj.MakePacket(person.Id, typeName, person)
			people = append(people, pass)
		}
		return people, nil
	}

	// try simple, non-filter version (all of a type)
	alwaysOkay := func(json string) bool { return true }
	filter := sj.Filter{Field: "id", Value: "per0000001", Compare: sj.Eq}

	// NOTE: count is entire list - in case list has to be chunked
	// could maybe default to size of listMaker results?
	intake := sj.IntakeConfig{TypeName: typeName, Count: 2, ChunkSize: 1, ListMaker: listMaker}
	// filter is only moving one record
	move := sj.TrajectConfig{TypeName: typeName, Validator: alwaysOkay, Filter: &filter}

	err := sj.ScramjetIntake(intake, move)

	if err != nil {
		t.Errorf("err=%v\n", err)
	}

	// ... so there should only be one record moved over
	count := sj.ResourceCount(typeName)
	if count != 1 {
		t.Errorf("after import and delete should be 1 record - not :%d\n", count)
	}
}

// make sure count = 0, chunkSize = 0 still import (non offset list)
func TestDefaultedIntake(t *testing.T) {
	sj.ClearAllStaging()
	sj.ClearAllResources()
	typeName := "person"

	// typically this is how a list might be created
	dbList := func() []IntakePerson {
		person1 := IntakePerson{Id: "per0000001", Name: "Test1"}
		person2 := IntakePerson{Id: "per0000002", Name: "Test2"}
		return []IntakePerson{person1, person2}
	}

	listMaker := func(i int) ([]sj.Storeable, error) {
		var people []sj.Storeable
		for _, person := range dbList() {
			pass := sj.MakePacket(person.Id, typeName, person)
			people = append(people, pass)
		}
		return people, nil
	}

	// try simple, non-filter version (all of a type)
	alwaysOkay := func(json string) bool { return true }
	// NOTE: count is entire list - in case list has to be chunked
	intake := sj.IntakeConfig{TypeName: typeName, ListMaker: listMaker}
	move := sj.TrajectConfig{TypeName: typeName, Validator: alwaysOkay}

	err := sj.ScramjetIntake(intake, move)

	if err != nil {
		t.Errorf("err=%v\n", err)
	}

	count := sj.ResourceCount(typeName)
	if count != 2 {
		t.Errorf("after import should be 2 records - not :%d\n", count)
	}
}

func TestRemoveByStub(t *testing.T) {
	sj.ClearAllStaging()
	sj.ClearAllResources()
	typeName := "person"

	// typically this is how a list might be created
	dbList := func() []IntakePerson {
		person1 := IntakePerson{Id: "per0000001", Name: "Test1"}
		person2 := IntakePerson{Id: "per0000002", Name: "Test2"}
		return []IntakePerson{person1, person2}
	}

	listMaker := func(i int) ([]sj.Storeable, error) {
		var people []sj.Storeable
		for _, person := range dbList() {
			pass := sj.MakePacket(person.Id, typeName, person)
			people = append(people, pass)
		}
		return people, nil
	}

	// try simple, non-filter version (all of a type)
	alwaysOkay := func(json string) bool { return true }

	intake := sj.IntakeConfig{TypeName: typeName, ListMaker: listMaker}
	move := sj.TrajectConfig{TypeName: typeName, Validator: alwaysOkay}

	err := sj.ScramjetIntake(intake, move)
	if err != nil {
		t.Errorf("err=%v\n", err)
	}
	// should have two records now ...
	// try removing one
	stub := sj.MakeStub("per0000001", "person")
	err = sj.RemoveRecords(stub)
	count := sj.ResourceCount(typeName)
	if count != 1 {
		t.Errorf("after remove should be 1 record - not :%d\n", count)
	}
}

func TestRemoveMultiple(t *testing.T) {
	sj.ClearAllStaging()
	sj.ClearAllResources()
	typeName := "person"

	// typically this is how a list might be created
	dbList := func() []IntakePerson {
		person1 := IntakePerson{Id: "per0000001", Name: "Test1"}
		person2 := IntakePerson{Id: "per0000002", Name: "Test2"}
		person3 := IntakePerson{Id: "per0000003", Name: "Test3"}
		return []IntakePerson{person1, person2, person3}
	}

	listMaker := func(i int) ([]sj.Storeable, error) {
		var people []sj.Storeable
		for _, person := range dbList() {
			pass := sj.MakePacket(person.Id, typeName, person)
			people = append(people, pass)
		}
		return people, nil
	}

	// try simple, non-filter version (all of a type)
	alwaysOkay := func(json string) bool { return true }

	intake := sj.IntakeConfig{TypeName: typeName, ListMaker: listMaker}
	move := sj.TrajectConfig{TypeName: typeName, Validator: alwaysOkay}

	err := sj.ScramjetIntake(intake, move)
	if err != nil {
		t.Errorf("err=%v\n", err)
	}
	// should have two records now ...
	// try removing one
	var stubs []sj.Stub
	stub1 := sj.MakeStub("per0000001", "person")
	stub2 := sj.MakeStub("per0000002", "person")
	stubs = append(stubs, stub1, stub2)

	err = sj.RemoveRecords(stubs...)
	count := sj.ResourceCount(typeName)
	if count != 1 {
		t.Errorf("after remove should be 1 record - not :%d\n", count)
	}
}

func TestRemoveAll(t *testing.T) {
	sj.ClearAllStaging()
	sj.ClearAllResources()
	typeName := "person"

	// typically this is how a list might be created
	dbList := func() []IntakePerson {
		person1 := IntakePerson{Id: "per0000001", Name: "Test1"}
		return []IntakePerson{person1}
	}

	listMaker := func(i int) ([]sj.Storeable, error) {
		var people []sj.Storeable
		for _, person := range dbList() {
			pass := sj.MakePacket(person.Id, typeName, person)
			people = append(people, pass)
		}
		return people, nil
	}

	// everything valid in test
	alwaysOkay := func(json string) bool { return true }

	intake := sj.IntakeConfig{TypeName: typeName, ListMaker: listMaker}
	move := sj.TrajectConfig{TypeName: typeName, Validator: alwaysOkay}

	err := sj.ScramjetIntake(intake, move)
	if err != nil {
		t.Errorf("err=%v\n", err)
	}

	// import another type - with a filterable attribute
	typeName2 := "newsfeed"
	dbList2 := func() []IntakeNewsfeed {
		new1 := IntakeNewsfeed{Id: "new1", PersonId: "per0000001", Title: "Testing1"}
		new2 := IntakeNewsfeed{Id: "new2", PersonId: "per0000001", Title: "Testing2"}
		new3 := IntakeNewsfeed{Id: "new3", PersonId: "per0000001", Title: "Testing3"}
		return []IntakeNewsfeed{new1, new2, new3}
	}

	listMaker2 := func(i int) ([]sj.Storeable, error) {
		var feeds []sj.Storeable
		for _, feed := range dbList2() {
			pass := sj.MakePacket(feed.Id, typeName2, feed)
			feeds = append(feeds, pass)
		}
		return feeds, nil
	}

	intake2 := sj.IntakeConfig{TypeName: typeName2, ListMaker: listMaker2}
	move2 := sj.TrajectConfig{TypeName: typeName2, Validator: alwaysOkay}

	// import news records in
	err = sj.ScramjetIntake(intake2, move2)
	if err != nil {
		t.Errorf("err=%v\n", err)
	}

	// make sure they made it
	count := sj.ResourceCount(typeName2)
	if count != 3 {
		t.Errorf("should be 3 records - not :%d\n", count)
	}

	filter := sj.Filter{Field: "personId", Value: "per0000001", Compare: sj.Eq}
	ids := func() ([]string, error) {
		//NOTE: the idea is that the source has no more records
		return []string{}, nil
	}
	// having the 'filter' allows deleting all
	out := sj.OutakeConfig{TypeName: typeName2,
		ListMaker: ids,
		Filter:    &filter,
	}
	err = sj.Eject(out)
	if err != nil {
		t.Errorf("err=%v\n", err)
	}

	// should be 0 records now
	count2 := sj.ResourceCount(typeName2)
	if count2 > 0 {
		t.Errorf("should be 0 records - not :%d\n", count2)
	}
}
