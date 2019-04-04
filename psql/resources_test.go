package psql_test

import (
	"encoding/json"
	"fmt"
	"testing"

	si "gitlab.oit.duke.edu/scholars/staging_importer"
	"gitlab.oit.duke.edu/scholars/staging_importer/psql"
)

func TestResourcesIngest(t *testing.T) {
	// NOTE: this is kind of re-hash of test in staging_test
	psql.ClearAllStaging()
	psql.ClearAllResources()
	typeName := "person"

	person1 := TestPerson{Id: "per0000001", Name: "Test1"}
	person2 := TestPerson{Id: "per0000002", Name: "Test2"}

	people := []si.Identifiable{person1, person2}
	err := psql.StashTypeStaging(typeName, people...)

	if err != nil {
		fmt.Println("could not save")
		t.Errorf("err=%v\n", err)
	}

	alwaysOkay := func(json string) bool { return true }
	valid, _ := psql.FilterTypeStaging(typeName, alwaysOkay)

	// mark them so we know they are processed and okay to go
	// into resources table
	psql.BatchMarkValidInStaging(valid)
	list := psql.RetrieveValidStaging(typeName)

	// now take that list and move to resources
	for _, res := range list {
		fmt.Println(res)

		per := &TestPerson{}
		err := json.Unmarshal(res.Data, per)
		if err != nil {
			t.Error("error unmarshalling json")
		}
		psql.SaveResource(per, typeName)
	}

	// TODO: need a better way to limit to updates
	stashed := psql.RetrieveResourceType(typeName, false)
	// NOTE: not marked in db column yet
	if len(stashed) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
}
