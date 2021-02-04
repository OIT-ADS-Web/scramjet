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
	JustTest  bool
	TypeName  string
	ListMaker OutakeListMaker
}

// TODO: shouldn't this return error if there is a problem?
func ProcessOutake(proc OutakeProcessConfig) {
	sourceData := proc.ListMaker()
	destData := make([]string, 0)
	err, resources := RetrieveTypeResources(proc.TypeName)
	if err != nil {
		fmt.Printf("couldn't retrieve list of %s\n", proc.TypeName)
	}
	if len(sourceData) == 0 {
		fmt.Printf("0 source records found - this would delete all %s records!\n", proc.TypeName)
		return
	}

	for _, res := range resources {
		destData = append(destData, res.Id)
	}
	extras := Difference(destData, sourceData)

	fmt.Printf("found %d extras\n", len(extras))
	deletes := make([]Identifiable, 0)
	for _, id := range extras {
		deletes = append(deletes, Stub{Id: Identifier{Id: id, Type: proc.TypeName}})
	}

	if !proc.JustTest {
		// NOTE: this is just marking them
		err = BulkAddStagingForDelete(deletes...)
		if err != nil {
			fmt.Println("could not mark for delete")
		}
	} else {
		fmt.Printf("would mark these:%s\n", deletes)
	}
}
