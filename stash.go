package scramjet

import (
	"errors"
	"fmt"
)

type IntakeListMaker func(int) ([]Storeable, error)

type ProgressChecker func(int)

type DeleteChecker func([]string)

type JustTestingInspector func(...interface{})
type IntakeConfig struct {
	TypeName  string
	ListMaker IntakeListMaker
	JustTest  bool
	Count     int
	ChunkSize int
}

func IntakeInChunks(ins IntakeConfig) error {
	var err error
	logger := *GetConfig().Logger
	for i := 0; i < ins.Count; i += ins.ChunkSize {
		msg := fmt.Sprintf("> retrieving %d-%d of %d\n", i, i+ins.ChunkSize, ins.Count)
		logger.Debug(msg)
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
			msg := fmt.Sprintf("would save:%#v\n", list)
			logger.Info(msg)
		}
	}
	return err
}

// maybe interface instead of func type in struct?
type OutakeListMaker func() ([]string, error)

type ResourceListMaker func() ([]Resource, error)

// NOTE: this is mostly the same as DiffProcessConfig
type OutakeProcessConfig struct {
	TypeName  string
	ListMaker OutakeListMaker
	JustTest  bool
	Checker   DeleteChecker
	Inspector JustTestingInspector
}

func ProcessOutake(config OutakeProcessConfig) error {
	// NOTE: for comparing source data of *all* with existing *all*
	existing := func() ([]Resource, error) {
		return RetrieveTypeResources(config.TypeName)
	}
	diffConfig := DiffProcessConfig{
		TypeName:          config.TypeName,
		ListMaker:         config.ListMaker,
		ExistingListMaker: existing,
		Inspector:         config.Inspector,
		Checker:           config.Checker,
	}
	return ProcessDiff(diffConfig)
}

type ExistingListMaker func() ([]Resource, error)

// to look for diffs for duid (for instance) both lists have to be sent in
type DiffProcessConfig struct {
	TypeName          string
	ExistingListMaker ExistingListMaker
	ListMaker         OutakeListMaker
	JustTest          bool
	Checker           DeleteChecker
	Inspector         JustTestingInspector
}

func ProcessDiff(config DiffProcessConfig) error {
	sourceData, err := config.ListMaker()
	if err != nil {
		msg := fmt.Sprintf("couldn't make list sent in for %s\n", config.TypeName)
		return errors.New(msg)
	}

	resources, err := config.ExistingListMaker()

	if err != nil {
		msg := fmt.Sprintf("couldn't retrieve list of %s\n", config.TypeName)
		return errors.New(msg)
	}
	return FlagDeletes(sourceData, resources, config)
}

func FlagDeletes(sourceDataIds []string, existingData []Resource, config DiffProcessConfig) error {
	typeName := config.TypeName
	justTest := config.JustTest
	inspector := config.Inspector
	checker := config.Checker

	destData := make([]string, 0)

	if len(sourceDataIds) == 0 && len(existingData) > 0 {
		msg := fmt.Sprintf("0 source records found - this would delete all %s records!\n", typeName)
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

	if checker != nil {
		checker(extras)
	}

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
		if inspector != nil {
			inspector(deletes)
		}
	}
	// return counts? or entire list?
	return nil
}
