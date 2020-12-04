package staging_importer

import (
	"fmt"
)

type StasherOptions struct {
	FlushSize int
}

// NOTE: this is an idea, not used or fleshed out yet
// looking for a facade to hide implementation details
// or Staging/Resource type functions
type Stasher interface {
	Items() map[string][]Storeable
	AddItems(string, ...Storeable)
	StashItems()
	SetOptions(StasherOptions)
}

type stagingStasher struct {
	options StasherOptions
	list    map[string][]Storeable
}

func (s stagingStasher) Items() map[string][]Storeable {
	return s.list
}

func NewStasher() Stasher {
	// NOTE: making default big - but also not even utilizing yet
	options := StasherOptions{FlushSize: 1000000}
	stashMap := make(map[string][]Storeable)
	return &stagingStasher{
		list:    stashMap,
		options: options,
	}
}

func (s stagingStasher) AddItems(typeName string, objs ...Storeable) {
	// NOTE: might want to add something here to 'BulkAddStaging'
	// if size is > FlushSize (at some point)
	s.list[typeName] = append(s.list[typeName], objs...)
}

func (s stagingStasher) SetOptions(opts StasherOptions) {
	s.options = opts
}

// moves into database
func (s stagingStasher) StashItems() {
	for k, v := range s.Items() {
		fmt.Printf("**** %s *****\n", k)
		for _, item := range v {
			fmt.Printf("->%s\n", item.Identifier())
		}
		err := BulkAddStaging(v...)
		if err != nil {
			fmt.Printf("saving error: %v\n", err)
		}
	}
}

func (s stagingStasher) DeleteItems() {
	for k, v := range s.Items() {
		fmt.Printf("**** %s *****\n", k)
		ids := make([]Identifiable, 0)
		for _, item := range v {
			fmt.Printf("->%s\n", item.Identifier())
			stub := Stub{Id: item.Identifier()}
			ids = append(ids, stub)
		}
		err := BulkAddStagingForDelete(ids...)
		if err != nil {
			fmt.Printf("saving error: %v\n", err)
		}
	}
}
