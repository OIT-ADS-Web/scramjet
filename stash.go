package staging_importer

import (
	"errors"
	"fmt"
)

type IntakeListMaker func(int) ([]Storeable, error)
type ChunkableIntakeConfig struct {
	Count     int
	ChunkSize int
	JustTest  bool
	TypeName  string
	ListMaker IntakeListMaker
	//ChunkCallback
}

type Skipped struct {
	Identifier Identifier
	Err        error
}

// add some callbacks?
func IntakeInChunks(ins ChunkableIntakeConfig) error {
	var err error
	for i := 0; i < ins.Count; i += ins.ChunkSize {
		// TODO: some way to print out status as running?  callback?

		list, err := ins.ListMaker(i)
		if err != nil {
			return err
		}
		if !ins.JustTest {
			err = BulkAddStaging(list...)
			if err != nil {
				return err
			}
		} else {
			// TODO: something better here for 'justTest'?
			fmt.Printf("would save:%s", list)
		}
	}
	return err
}

type OutakeListMaker func() ([]string, error)
type OutakeProcessConfig struct {
	TypeName  string
	ListMaker OutakeListMaker
	JustTest  bool
}

// TODO: shouldn't this return error if there is a problem?
func ProcessOutake(proc OutakeProcessConfig) error {
	sourceData, err := proc.ListMaker()
	if err != nil {
		msg := fmt.Sprintf("couldn't make list sent in for %s\n", proc.TypeName)
		return errors.New(msg)
	}
	// NOTE: for comparing source data of *all* with existing *all*
	resources, err := RetrieveTypeResources(proc.TypeName)
	if err != nil {
		msg := fmt.Sprintf("couldn't retrieve list of %s\n", proc.TypeName)
		return errors.New(msg)
	}
	return flagDeletes(sourceData, resources, proc.TypeName, proc.JustTest)
}

type ExistingListMaker func() (error, []Resource)

/*
func ProcessCompare(proc DiffProcessConfig) error {
	// NOTE: idea is to compare two limited lists such as overview per duid
	// instead of looking for *all* extra overviews
	sourceData, err := proc.ListMaker()
	if err != nil {
		msg := fmt.Sprintf("couldn't make list sent in for %s\n", proc.TypeName)
		return errors.New(msg)
	}
	err, resources := proc.ExistingListMaker()
	if err != nil {
		msg := fmt.Sprintf("couldn't retrieve list of %s\n", proc.TypeName)
		return errors.New(msg)
	}
	return flagDeletes(sourceData, resources, proc.TypeName, proc.JustTest)
}
*/

// to look for diffs for duid (for instance) both lists have to be sent in
type DiffProcessConfig struct {
	TypeName          string
	ExistingListMaker ExistingListMaker
	ListMaker         OutakeListMaker
	JustTest          bool
}

func flagDeletes(sourceDataIds []string, existingData []Resource, typeName string, justTest bool) error {
	destData := make([]string, 0)

	if len(sourceDataIds) == 0 && len(existingData) > 0 {
		msg := fmt.Sprintf("0 source records found - this would delete all %s records!\n", typeName)
		return errors.New(msg)
	} else if len(sourceDataIds) > 0 && len(existingData) == 0 {
		msg := "no existing records to compare against"
		return errors.New(msg)
	} else if len(sourceDataIds) == 0 && len(existingData) == 0 {
		msg := "0 record to compare on either side!"
		return errors.New(msg)
	}

	if len(existingData) > 0 {
		peek := existingData[0]
		// NOTE: function intent is to be comparing ids/per type - not just
		// any list of ids
		if peek.Type != typeName {
			msg := fmt.Sprintf("unexpected type in existing data (%s vs %s)!\n", peek.Type, typeName)
			return errors.New(msg)
		}
	}

	for _, res := range existingData {
		destData = append(destData, res.Id)
	}
	extras := Difference(destData, sourceDataIds)

	// TODO: maybe send count as return value
	fmt.Printf("found %d extras\n", len(extras))
	deletes := make([]Identifiable, 0)
	for _, id := range extras {
		// how to get type?
		deletes = append(deletes, Stub{Id: Identifier{Id: id, Type: typeName}})
	}

	if !justTest {
		// NOTE: this is just marking them, not deleting at this stage
		err := BulkAddStagingForDelete(deletes...)
		if err != nil {
			msg := fmt.Sprintf("could not mark for delete: %s", err)
			return errors.New(msg)
		}
	} else {
		// callback?
		fmt.Printf("would mark these:%s\n", deletes)
	}
	// return counts? or entire list?
	return nil
}
