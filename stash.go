package staging_importer

import (
	"fmt"
)

type IntakeListMaker func(int) []Storeable
type ChunkableIntakeConfig struct {
	Count     int
	ChunkSize int
	JustTest  bool
	TypeName  string
	ListMaker IntakeListMaker
}

func IntakeInChunks(ins ChunkableIntakeConfig) {
	for i := 0; i < ins.Count; i += ins.ChunkSize {
		fmt.Printf("> retrieving %d-%d of %d\n", i, i+ins.ChunkSize, ins.Count)
		list := ins.ListMaker(i)
		if !ins.JustTest {
			err := BulkAddStaging(list...)
			if err != nil {
				fmt.Println("could not save as list")
			}
		} else {
			fmt.Printf("would save:%s", list)
		}
	}
}

type OutakeListMaker func() []string
type OutakeProcessConfig struct {
	TypeName  string
	ListMaker OutakeListMaker
	JustTest  bool
}

// TODO: shouldn't this return error if there is a problem?
func ProcessOutake(proc OutakeProcessConfig) {
	sourceData := proc.ListMaker()

	// NOTE: for comparing source data of *all* with existing *all*
	err, resources := RetrieveTypeResources(proc.TypeName)
	if err != nil {
		fmt.Printf("couldn't retrieve list of %s\n", proc.TypeName)
	}
	flagDeletes(sourceData, resources, proc.TypeName, proc.JustTest)
}

type ExistingListMaker func() (error, []Resource)

func ProcessCompare(proc DiffProcessConfig) {
	// NOTE: idea is to compare two limited lists such as overview per duid
	// instead of looking for *all* extra overviews
	sourceData := proc.ListMaker()
	err, resources := proc.ExistingListMaker()
	if err != nil {
		fmt.Printf("couldn't retrieve list of %s\n", proc.TypeName)
	}
	flagDeletes(sourceData, resources, proc.TypeName, proc.JustTest)
}

// to look for diffs for duid (for instance) both lists have to be sent in
type DiffProcessConfig struct {
	TypeName          string
	ExistingListMaker ExistingListMaker
	ListMaker         OutakeListMaker
	JustTest          bool
}

func flagDeletes(sourceDataIds []string, existingData []Resource, typeName string, justTest bool) {
	destData := make([]string, 0)

	if len(sourceDataIds) == 0 && len(existingData) > 0 {
		fmt.Printf("0 source records found - this would delete all %s records!\n", typeName)
		return
	} else if len(sourceDataIds) > 0 && len(existingData) == 0 {
		fmt.Println("no existing records to compare against")
	} else if len(sourceDataIds) == 0 && len(existingData) == 0 {
		fmt.Println("0 record to compare on either side!")
		return
	}

	if len(existingData) > 0 {
		peek := existingData[0]
		// NOTE: function intent is to be comparing ids/per type - not just
		// any list of ids
		if peek.Type != typeName {
			fmt.Printf("unexpected type in existing data (%s vs %s)!\n", peek.Type, typeName)
			return
		}
	}

	for _, res := range existingData {
		destData = append(destData, res.Id)
	}
	extras := Difference(destData, sourceDataIds)

	fmt.Printf("found %d extras\n", len(extras))
	deletes := make([]Identifiable, 0)
	for _, id := range extras {
		// how to get type?
		deletes = append(deletes, Stub{Id: Identifier{Id: id, Type: typeName}})
	}

	if !justTest {
		// NOTE: this is just marking them
		err := BulkAddStagingForDelete(deletes...)
		if err != nil {
			fmt.Println("could not mark for delete")
		}
	} else {
		fmt.Printf("would mark these:%s\n", deletes)
	}
}
