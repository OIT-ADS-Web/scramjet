package scramjet_test

import (
	"fmt"
	"testing"
	"time"

	sj "github.com/OIT-ADS-Web/scramjet"
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
	valid, _, _ := sj.FilterTypeStaging(typeName, alwaysOkay)

	// mark them so we know they are processed and okay to go
	// into resources table
	err = sj.BatchMarkValidInStaging(valid)
	if err != nil {
		t.Error("error marking records valid")
	}
	list, err := sj.RetrieveValidStaging(typeName)
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
	valid, _, _ := sj.FilterTypeStaging(typeName, alwaysOkay)
	if len(valid) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
	err = sj.BatchMarkValidInStaging(valid)
	// should be two marked as 'valid' now
	if err != nil {
		t.Error("error marking records valid")
	}
	list, err := sj.RetrieveValidStaging(typeName)
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
	valid, _, _ := sj.FilterTypeStaging(typeName, alwaysOkay)
	if len(valid) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
	err = sj.BatchMarkValidInStaging(valid)
	// should be two marked as 'valid' now
	if err != nil {
		t.Error("error marking records valid")
	}
	list, err := sj.RetrieveValidStaging(typeName)
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
	valid, _, _ := sj.FilterTypeStaging(typeName, alwaysOkay)
	if len(valid) != 1 {
		t.Error("did not retrieve 1 and only 1 record")
	}
	err = sj.BatchMarkValidInStaging(valid)
	// should be one marked as 'valid' now
	if err != nil {
		t.Error("error marking records valid")
	}
	// now move to resources table since they are valid
	list, err := sj.RetrieveValidStaging(typeName)
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

func TestMarkUpdates(t *testing.T) {
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
	valid, _, _ := sj.FilterTypeStaging(typeName, alwaysOkay)
	if len(valid) != 1 {
		t.Error("did not retrieve 1 and only 1 record")
	}
	err = sj.BatchMarkValidInStaging(valid)
	// should be one marked as 'valid' now
	if err != nil {
		t.Error("error marking records valid")
	}
	// now move to resources table since they are valid
	list, err := sj.RetrieveValidStaging(typeName)
	err = sj.BulkMoveStagingTypeToResources(typeName, list...)

	rec1, err := sj.RetrieveSingleResource("per0000001", "person")
	fmt.Printf("res1=%s\n", rec1.UpdatedAt.Format(time.UnixDate))
	firstUpdatedAt := rec1.UpdatedAt

	if err != nil {
		t.Error("error finding resource")
	}

	// now update - same id and type
	time.Sleep(2 * time.Second) // sleep for 2 seconds

	person2 := TestPerson{Id: "per0000001", Name: "Test123updated"}
	pass2 := sj.Packet{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}
	people2 := []sj.Storeable{pass2}

	if err != nil {
		t.Errorf("err=%v\n", err)
	}
	err = sj.StashStaging(people2...)

	valid2, _, _ := sj.FilterTypeStaging(typeName, alwaysOkay)
	if len(valid2) != 1 {
		t.Error("did not retrieve 1 and only 1 record")
	}
	err = sj.BatchMarkValidInStaging(valid2)
	// should be one marked as 'valid' now
	if err != nil {
		t.Error("error marking records valid")
	}
	// now move to resources table since they are valid
	list2, err := sj.RetrieveValidStaging(typeName)
	err = sj.BulkMoveStagingTypeToResources(typeName, list2...)

	rec2, err := sj.RetrieveSingleResource("per0000001", "person")
	fmt.Printf("res2=%s\n", rec2.UpdatedAt.Format(time.UnixDate))
	if err != nil {
		t.Error("error finding resource")
	}

	if !rec2.UpdatedAt.After(firstUpdatedAt) {
		msg := fmt.Sprintf("did not update resource %s < %s",
			rec2.UpdatedAt.Format(time.UnixDate), firstUpdatedAt.Format(time.UnixDate))
		t.Error(msg)
	}

}

func TestNotMarkUpdates(t *testing.T) {
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
	valid, _, _ := sj.FilterTypeStaging(typeName, alwaysOkay)
	if len(valid) != 1 {
		t.Error("did not retrieve 1 and only 1 record")
	}
	err = sj.BatchMarkValidInStaging(valid)
	// should be one marked as 'valid' now
	if err != nil {
		t.Error("error marking records valid")
	}
	// now move to resources table since they are valid
	list, err := sj.RetrieveValidStaging(typeName)
	err = sj.BulkMoveStagingTypeToResources(typeName, list...)

	rec1, err := sj.RetrieveSingleResource("per0000001", "person")
	fmt.Printf("res1=%s\n", rec1.UpdatedAt.Format(time.UnixDate))
	firstUpdatedAt := rec1.UpdatedAt

	if err != nil {
		t.Error("error finding resource")
	}

	// now update - same id and type
	time.Sleep(2 * time.Second) // sleep for 2 seconds

	// NOTE: it's the same data
	person2 := TestPerson{Id: "per0000001", Name: "Test1"}
	pass2 := sj.Packet{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}
	people2 := []sj.Storeable{pass2}

	if err != nil {
		t.Errorf("err=%v\n", err)
	}
	err = sj.StashStaging(people2...)

	valid2, _, _ := sj.FilterTypeStaging(typeName, alwaysOkay)
	if len(valid2) != 1 {
		t.Error("did not retrieve 1 and only 1 record")
	}
	err = sj.BatchMarkValidInStaging(valid2)
	// should be one marked as 'valid' now
	if err != nil {
		t.Error("error marking records valid")
	}
	// now move to resources table since they are valid
	list2, err := sj.RetrieveValidStaging(typeName)
	err = sj.BulkMoveStagingTypeToResources(typeName, list2...)

	rec2, err := sj.RetrieveSingleResource("per0000001", "person")
	fmt.Printf("res2=%s\n", rec2.UpdatedAt.Format(time.UnixDate))
	if err != nil {
		t.Error("error finding resource")
	}

	if rec2.UpdatedAt != firstUpdatedAt {
		msg := fmt.Sprintf("updated resource %s < %s",
			rec2.UpdatedAt.Format(time.UnixDate), firstUpdatedAt.Format(time.UnixDate))
		t.Error(msg)
	}

}

func TestFilteredList(t *testing.T) {
	sj.ClearAllStaging()
	sj.ClearAllResources()
	typeName := "person"

	person1 := TestPersonExtended{Id: "per0000001", Name: "Test1", ExternalId: "x100"}
	person2 := TestPersonExtended{Id: "per0000002", Name: "Test2", ExternalId: "x200"}

	pass1 := sj.Packet{Id: sj.Identifier{Id: person1.Id, Type: typeName}, Obj: person1}
	pass2 := sj.Packet{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}
	people := []sj.Storeable{pass1, pass2}

	// 1. put all in staging
	err := sj.StashStaging(people...)

	if err != nil {
		t.Errorf("err=%v\n", err)
	}

	alwaysOkay := func(json string) bool { return true }

	filter := sj.Filter{Field: "externalId", Value: "x200", Compare: sj.Eq}
	// 2. but only get one out
	valid, _, _ := sj.FilterTypeStagingByQuery(typeName, filter, alwaysOkay)
	if len(valid) != 1 {
		//fmt.Printf("%s\n", valid)
		t.Error("did not retrieve 1 and only 1 record")
	}

	err = sj.BatchMarkValidInStaging(valid)
	// should be one marked as 'valid' now
	if err != nil {
		t.Error("error marking records valid")
	}

	// now move to resources table since they are valid
	//list2 := sj.RetrieveValidStaging(typeName)
	list2, err := sj.RetrieveValidStagingFiltered(typeName, filter)
	err = sj.BulkMoveStagingTypeToResources(typeName, list2...)

	records, err := sj.RetrieveTypeResourcesByQuery("person", filter)
	if len(records) != 1 {
		t.Error("did not retrieve 1 and only 1 record from resources")
	}
}

func TestAdvancedFilteredList(t *testing.T) {
	sj.ClearAllStaging()
	sj.ClearAllResources()

	pub1 := TestPublication{Id: "pub0001", Title: "PublicationTest"}
	auth1 := TestAuthorship{Id: "auth0001", PublicationId: "pub0001", PersonId: "per0000001"}
	person1 := TestPerson{Id: "per0000001", Name: "Test1"}

	pass1 := sj.Packet{Id: sj.Identifier{Id: pub1.Id, Type: "publication"}, Obj: pub1}
	pass2 := sj.Packet{Id: sj.Identifier{Id: auth1.Id, Type: "authorship"}, Obj: auth1}
	pass3 := sj.Packet{Id: sj.Identifier{Id: person1.Id, Type: "person"}, Obj: person1}
	records := []sj.Storeable{pass1, pass2, pass3}

	// 1. put all in staging
	err := sj.StashStaging(records...)

	if err != nil {
		t.Errorf("err=%v\n", err)
	}

	alwaysOkay := func(json string) bool { return true }

	subFilter := sj.SubFilter{Typename: "authorship",
		MatchField:  "personId",
		Value:       "per0000001",
		ParentMatch: "publicationId",
	}

	filter1 := sj.Filter{Field: "id", SubFilter: &subFilter, Compare: sj.In}

	// 2. but only get one out - using subfilter ...
	valid1, _, _ := sj.FilterTypeStagingByQuery("publication", filter1, alwaysOkay)
	if len(valid1) != 1 {
		msg := fmt.Sprintf("did not retrieve 1 and only 1 record from staging (%d)\n", len(valid1))
		t.Error(msg)
	}

	err = sj.BatchMarkValidInStaging(valid1)
	// should be one marked as 'valid' now
	if err != nil {
		t.Error("error marking records valid")
	}
	// NOTE: at this point 1

	// now move to resources table since they are valid
	list1, err := sj.RetrieveValidStagingFiltered("publication", filter1)
	if len(list1) != 1 {
		msg := fmt.Sprintf("did not find 1 and only 1 record to move from staging (%d)\n", len(list1))
		t.Error(msg)
	}
	err = sj.BulkMoveStagingToResourcesByFilter("publication", filter1, list1...)

	if err != nil {
		t.Error("error moving records from staging to resources")
	}

	// NOTE: at this point there is the filtered publication list in resources
	// but there is no 'authorship' record (yet) to verify the filter
	filter2 := sj.Filter{Field: "personId", Value: "per0000001", Compare: sj.Eq}

	valid2, _, _ := sj.FilterTypeStagingByQuery("authorship", filter2, alwaysOkay)
	if len(valid2) != 1 {
		msg := fmt.Sprintf("did not retrieve 1 and only 1 record from staging (%d)\n", len(valid2))
		t.Error(msg)
	}
	err = sj.BatchMarkValidInStaging(valid2)

	// now authorships should be marked as valid
	list2, err := sj.RetrieveValidStagingFiltered("authorship", filter2)
	if len(list2) != 1 {
		msg := fmt.Sprintf("did not find 1 and only 1 record to move from staging (%d)\n", len(list2))
		t.Error(msg)
	}

	// move over to resources ...
	err = sj.BulkMoveStagingToResourcesByFilter("authorship", filter2, list2...)
	if err != nil {
		t.Errorf("error moving records;%s\n", err)
	}
	count := sj.StagingCount()
	// NOTE: 'person' record is still in staging (have not deleted)
	if count != 1 {
		t.Errorf("error clearing out staging after processing - count=%d\n", count)
	}

	// 3. now finally can verify filter1 in 'resources' because authorship has made it over
	results, err := sj.RetrieveTypeResourcesByQuery("publication", filter1)
	if len(results) != 1 {
		msg := fmt.Sprintf("did not retrieve 1 and only 1 record from resources (%d)\n", len(results))
		t.Error(msg)
	}
}

func TestDiffOutExtras(t *testing.T) {
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
	valid, _, _ := sj.FilterTypeStaging(typeName, alwaysOkay)
	if len(valid) != 2 {
		t.Error("did not retrieve 2 and only 2 record")
	}
	err = sj.BatchMarkValidInStaging(valid)
	// should be two marked as 'valid' now
	if err != nil {
		t.Error("error marking records valid")
	}
	list, err := sj.RetrieveValidStaging(typeName)
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

	listMaker := func() ([]string, error) {
		return []string{"per0000001"}, nil
	}
	current := func() ([]sj.Resource, error) {
		return sj.RetrieveTypeResources(typeName)
	}
	finder := sj.DiffProcessConfig{
		ListMaker:         listMaker,
		TypeName:          typeName,
		ExistingListMaker: current,
	}
	// mark them for delete
	err = sj.ProcessDiff(finder)

	if err != nil {
		t.Errorf("unable to mark records for delete:%s", err)
	}

	// then delete
	err = sj.BulkRemoveStagingDeletedFromResources(typeName)

	if err != nil {
		t.Errorf("unable to delete from resources:%s", err)
	}
	// should be one now, not two
	count := sj.ResourceCount(typeName)
	if count != 1 {
		t.Errorf("after delete, should be 1 record not %d\n", count)
	}
}
