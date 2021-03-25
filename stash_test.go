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
