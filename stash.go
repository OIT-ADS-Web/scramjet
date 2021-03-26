package scramjet

import (
	"errors"
	"fmt"
)

// the parameter (int) if offset
type IntakeListMaker func(int) ([]Storeable, error)

//type ProgressChecker func(int)
//type DeleteChecker func([]string)

type IntakeConfig struct {
	TypeName  string
	ListMaker IntakeListMaker
	Count     int
	ChunkSize int
}

/*
func BasicIntakeConfig(typeName string, listMaker IntakeListMaker) IntakeConfig {
	return IntakeConfig{
		TypeName:  typeName,
		ListMaker: listMaker,
		Count:     0,
		ChunkSize: 1000,
	}
}

func ChunkedIntakeConfig(typeName string, listMaker IntakeListMaker, count int, chunkSize int) IntakeConfig {
	return IntakeConfig{
		TypeName:  typeName,
		ListMaker: listMaker,
		Count:     count,
		ChunkSize: chunkSize,
	}
}
*/

type TrajectConfig struct {
	TypeName  string
	Validator ValidatorFunc
	Filter    *Filter // could this be transaction only?
}

type OutakeConfig struct {
	TypeName  string
	ListMaker OutakeListMaker
	Filter    *Filter // could this be transaction only? don't think so
}

func Scramjet(in IntakeConfig, process TrajectConfig, out OutakeConfig) error {
	err := Inject(in)
	if err != nil {
		return err
	}
	err = Traject(process)
	if err != nil {
		return err
	}
	err = Eject(out)
	if err != nil {
		return err
	}
	return nil
}

func ScramjetIntake(in IntakeConfig, process TrajectConfig) error {
	err := Inject(in)
	if err != nil {
		return err
	}
	err = Traject(process)
	if err != nil {
		return err
	}
	return nil
}

func ScramjetOutake(out OutakeConfig) error {
	err := Eject(out)
	if err != nil {
		return err
	}
	return nil
}

func Inject(config IntakeConfig) error {
	return IntakeInChunks(config)
}

func Traject(config TrajectConfig) error {
	if config.Filter != nil {
		return TransferSubset(config.TypeName, *config.Filter, config.Validator)
	} else {
		return TransferAll(config.TypeName, config.Validator)
	}
}

func Eject(config OutakeConfig) error {
	err := ProcessOutake(config)
	if err != nil {
		return err
	}
	// how to differentiate diff, with out-take?
	if config.Filter != nil {
		// NOTE: right now json is {} so no way to actually filter
		err = BulkRemoveStagingDeletedFromResources(config.TypeName)
	} else {
		err = BulkRemoveStagingDeletedFromResources(config.TypeName)
	}
	if err != nil {
		return err
	}
	return nil
}

func TransferAll(typeName string, validator ValidatorFunc) error {
	err := ProcessTypeStaging(typeName, validator)
	if err != nil {
		return err
	}
	staging, err := RetrieveValidStaging(typeName)
	if err != nil {
		return err
	}
	err = BulkMoveStagingTypeToResources(typeName, staging...)
	if err != nil {
		return err
	}
	return nil
}

func TransferSubset(typeName string, filter Filter, validator ValidatorFunc) error {
	err := ProcessTypeStagingFiltered(typeName, filter, validator)
	if err != nil {
		return err
	}
	staging, err := RetrieveValidStagingFiltered(typeName, filter)
	if err != nil {
		return err
	}
	err = BulkMoveStagingToResourcesByFilter(typeName, filter, staging...)
	if err != nil {
		return err
	}
	return nil
}

func IntakeInChunks(ins IntakeConfig) error {
	var err error
	var logger = GetLogger()

	if ins.Count == 0 {
		msg := fmt.Sprintf("> retrieving records of %s in one call\n", ins.TypeName)
		logger.Debug(msg)
		offset := 0
		// just start at first record
		list, err := ins.ListMaker(offset)
		if err != nil {
			return err
		}
		err = BulkAddStaging(list...)
		if err != nil {
			return err
		}
		msg = fmt.Sprintf("> retrieved %d records\n", len(list))
		logger.Debug(msg)

	} else {
		for i := 0; i < ins.Count; i += ins.ChunkSize {
			msg := fmt.Sprintf("> retrieving %d-%d of %d\n", i, i+ins.ChunkSize, ins.Count)
			logger.Debug(msg)
			list, err := ins.ListMaker(i)
			if err != nil {
				return err
			}
			err = BulkAddStaging(list...)
			if err != nil {
				return err
			}
		}
		logger.Debug(fmt.Sprintf("> finished %s records\n", ins.TypeName))
	}
	return err
}

// maybe interface instead of func type in struct?
type OutakeListMaker func() ([]string, error)

type ResourceListMaker func() ([]Resource, error)

func ProcessOutake(config OutakeConfig) error {
	// NOTE: for comparing source data of *all* with existing *all*
	var existing ExistingListMaker
	if config.Filter != nil {
		existing = func() ([]Resource, error) {
			return RetrieveTypeResourcesByQuery(config.TypeName, *config.Filter)
		}
	} else {
		existing = func() ([]Resource, error) {
			return RetrieveTypeResources(config.TypeName)
		}
	}
	diffConfig := DiffProcessConfig{
		TypeName:          config.TypeName,
		ListMaker:         config.ListMaker,
		ExistingListMaker: existing,
	}
	return ProcessDiff(diffConfig)
}

type ExistingListMaker func() ([]Resource, error)

// to look for diffs for duid (for instance) both lists have to be sent in
type DiffProcessConfig struct {
	TypeName          string
	ExistingListMaker ExistingListMaker
	ListMaker         OutakeListMaker
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

	GetLogger().Debug(fmt.Sprintf("found =%d extras\n", len(extras)))

	deletes := make([]Identifiable, 0)
	for _, id := range extras {
		// how to get type?
		deletes = append(deletes, Stub{Id: Identifier{Id: id, Type: typeName}})
	}
	err := BulkAddStagingForDelete(deletes...)
	if err != nil {
		msg := fmt.Sprintf("could not mark for delete: %s", err)
		return errors.New(msg)
	}
	// return something else? counts? entire list?
	return nil
}

func MakePacket(id string, typeName string, obj interface{}) Packet {
	return Packet{
		Id:  Identifier{Id: id, Type: typeName},
		Obj: obj,
	}
}
