package scramjet_test

import (
	"testing"

	sj "github.com/OIT-ADS-Web/scramjet"
)

type IntakePerson struct {
	Id   string `json:"id"`
	Name string `json:"name"`
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

	// main function to do all 3 in one sequence
	err = sj.ScramjetOutake(outake)
	if err != nil {
		t.Errorf("err=%v\n", err)
	}

	count := sj.ResourceCount(typeName)
	if count != 1 {
		t.Errorf("after import and delete should be 1 record - not :%d\n", count)
	}
}