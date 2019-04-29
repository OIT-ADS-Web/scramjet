package staging_importer_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	sj "gitlab.oit.duke.edu/scholars/staging_importer"
)

// each usage would need it's own implementation of this
// maybe make it a type-mapper object of some sort
// and able to pass in to processor?
func makeStub(typeName string) (sj.UriAddressable, error) {
	switch typeName {
	case "person":
		return &TestPerson{}, nil
	}
	return nil, errors.New("No match")
}

func TestResourcesIngest(t *testing.T) {
	// NOTE: this is kind of re-hash of test in staging_test
	sj.ClearAllStaging()
	sj.ClearAllResources()
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

	// mark them so we know they are processed and okay to go
	// into resources table
	sj.BatchMarkValidInStaging(valid)
	list := sj.RetrieveValidStaging(typeName)

	// now take that list and move to resources
	for _, res := range list {
		fmt.Println(res)
		//per := &TestPerson{}
		per, err := makeStub(typeName)
		if err != nil {
			t.Error("error making struct")
		}
		err = json.Unmarshal(res.Data, per)
		if err != nil {
			t.Error("error unmarshalling json")
		}
		// one at a time
		sj.SaveResource(per, typeName)
	}

	// TODO: need a better way to limit to updates
	stashed := sj.RetrieveResourceType(typeName, false)
	// NOTE: not marked in db column yet
	if len(stashed) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
}