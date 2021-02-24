package staging_importer_test

import (
	"testing"

	sj "gitlab.oit.duke.edu/scholars/staging_importer"
)

func TestResourcesIngest(t *testing.T) {
	// NOTE: this is kind of re-hash of test in staging_test
	sj.ClearAllStaging()
	sj.ClearAllResources()
	typeName := "person"

	person1 := TestPerson{Id: "per0000001", Name: "Test1"}
	person2 := TestPerson{Id: "per0000002", Name: "Test2"}
	pass1 := sj.Packet{Id: sj.Identifier{Id: person1.Id, Type: typeName}, Obj: person1}
	pass2 := sj.Packet{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}
	people := []sj.Storeable{pass1, pass2}

	err := sj.StashStaging(people...)

	if err != nil {
		t.Errorf("err=%v\n", err)
	}

	alwaysOkay := func(json string) bool { return true }
	valid, _ := sj.FilterTypeStaging(typeName, alwaysOkay)

	// mark them so we know they are processed and okay to go
	// into resources table
	err = sj.BatchMarkValidInStaging(valid)
	if err != nil {
		t.Error("error marking records valid")
	}
	list := sj.RetrieveValidStaging(typeName)
	err = sj.BulkMoveStagingTypeToResources(typeName, list...)

	if err != nil {
		t.Error("error moving to resource table")
	}
	// TODO: need a better way to limit to updates
	stashed, err := sj.RetrieveTypeResources(typeName)
	if err != nil {
		t.Error("error stashing record")
	}
	// NOTE: not marked in db column yet
	if len(stashed) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
}

func TestBatchResources(t *testing.T) {
	// clear out staging here
	sj.ClearAllStaging()
	sj.ClearAllResources()
	typeName := "person"

	person1 := TestPerson{Id: "per0000001", Name: "Test1"}
	person2 := TestPerson{Id: "per0000002", Name: "Test2"}
	pass1 := sj.Packet{Id: sj.Identifier{Id: person1.Id, Type: typeName}, Obj: person1}
	pass2 := sj.Packet{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}
	people := []sj.Storeable{pass1, pass2}

	err := sj.StashStaging(people...)

	if err != nil {
		t.Errorf("err=%v\n", err)
	}

	alwaysOkay := func(json string) bool { return true }
	valid, _ := sj.FilterTypeStaging(typeName, alwaysOkay)
	if len(valid) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
	err = sj.BatchMarkValidInStaging(valid)
	// should be two marked as 'valid' now
	if err != nil {
		t.Error("error marking records valid")
	}
	list := sj.RetrieveValidStaging(typeName)
	err = sj.BulkMoveStagingTypeToResources(typeName, list...)

	// false = not updates only
	existing, err := sj.RetrieveTypeResources(typeName)
	if err != nil {
		t.Error("error stashing record")
	}
	if len(existing) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
}

func TestBatchDeleteResources(t *testing.T) {
	// clear out staging here
	err := sj.ClearAllStaging()
	if err != nil {
		t.Errorf("err=%v\n", err)
	}
	err = sj.ClearAllResources()
	if err != nil {
		t.Errorf("err=%v\n", err)
	}
	typeName := "person"

	person1 := TestPerson{Id: "per0000001", Name: "Test1"}
	person2 := TestPerson{Id: "per0000002", Name: "Test2"}
	pass1 := sj.Packet{Id: sj.Identifier{Id: person1.Id, Type: typeName}, Obj: person1}
	pass2 := sj.Packet{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}
	people := []sj.Storeable{pass1, pass2}

	err = sj.StashStaging(people...)

	if err != nil {
		t.Errorf("err=%v\n", err)
	}

	alwaysOkay := func(json string) bool { return true }
	valid, _ := sj.FilterTypeStaging(typeName, alwaysOkay)
	if len(valid) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
	err = sj.BatchMarkValidInStaging(valid)
	// should be two marked as 'valid' now
	if err != nil {
		t.Error("error marking records valid")
	}
	list := sj.RetrieveValidStaging(typeName)
	if len(valid) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
	// NOTE: this clears staging table
	err = sj.BulkMoveStagingTypeToResources(typeName, list...)

	// make sure they made it to begin with
	existing, err := sj.RetrieveTypeResources(typeName)
	if err != nil {
		t.Error("error stashing record")
	}
	if len(existing) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
	// now turn around and mark for delete
	err = sj.BulkAddStagingForDelete(valid...)

	if err != nil {
		t.Error("error marking records valid")
	}

	deletes, _ := sj.RetrieveDeletedStaging(typeName)
	if len(deletes) != 2 {
		t.Error("did not mark 2 records for delete")
	}

	// then delete
	err = sj.BulkRemoveStagingDeletedFromResources(typeName)
	if err != nil {
		t.Errorf("could not mark for delete;err=%v\n", err)
	}
	existing, err = sj.RetrieveTypeResources(typeName)
	if err != nil {
		t.Error("error retrieving record")
	}
	if len(existing) != 0 {
		t.Error("after delete, should not be any records")
	}
}

func TestDeleteResource(t *testing.T) {
	sj.ClearAllStaging()
	sj.ClearAllResources()
	typeName := "person"

	person1 := TestPerson{Id: "per0000001", Name: "Test1"}
	pass1 := sj.Packet{Id: sj.Identifier{Id: person1.Id, Type: typeName}, Obj: person1}
	people := []sj.Storeable{pass1}

	err := sj.StashStaging(people...)

	if err != nil {
		t.Errorf("err=%v\n", err)
	}

	alwaysOkay := func(json string) bool { return true }
	valid, _ := sj.FilterTypeStaging(typeName, alwaysOkay)
	if len(valid) != 1 {
		t.Error("did not retrieve 1 and only 1 record")
	}
	err = sj.BatchMarkValidInStaging(valid)
	// should be one marked as 'valid' now
	if err != nil {
		t.Error("error marking records valid")
	}
	// now move to resources table since they are valid
	list := sj.RetrieveValidStaging(typeName)
	err = sj.BulkMoveStagingTypeToResources(typeName, list...)

	// now it's time to delete one, same one we added - but only Id data
	person2 := TestPerson{Id: "per0000001"}
	// NOTE: could use 'Stub' here since it's only for delete
	pass2 := sj.Packet{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}

	deletes := []sj.Identifiable{pass2}

	err = sj.BulkAddStagingForDelete(deletes...)
	if err != nil {
		t.Errorf("error adding to staging (for delete):%s", err)
	}
	deleteCount := sj.StagingDeleteCount(typeName)
	if deleteCount == 0 {
		t.Error("after after adding to deletes, no deletes in table")
	}
	err = sj.BulkRemoveStagingDeletedFromResources(typeName)

	if err != nil {
		t.Errorf("unable to delete from resources:%s", err)
	}
	count := sj.ResourceCount(typeName)
	if count != 0 {
		t.Error("after delete, should not be any records")
	}

}
