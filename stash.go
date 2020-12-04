package staging_importer

import (
	"fmt"
)

type StasherOptions struct {
	FlushSize int
}

type Stasher interface {
	Items() map[string][]Identifiable
	AddItems(string, ...Identifiable)
	StashItems()
	SetOptions(StasherOptions)
}

/*
type StagingStasher interface {
	Items() map[string][]Identifiable
	AddItems(string, ...Identifiable)
	StashItems()
}
*/
type stagingStasher struct {
	options StasherOptions
	list    map[string][]Identifiable
}

func (s stagingStasher) Items() map[string][]Identifiable {
	return s.list
}

func NewStasher() Stasher {
	// NOTE: making default big - but also not even utilizing yet
	options := StasherOptions{FlushSize: 1000000}
	stashMap := make(map[string][]Identifiable)
	return &stagingStasher{
		list:    stashMap,
		options: options,
	}
}

func (s stagingStasher) AddItems(typeName string, objs ...Identifiable) {
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
		for _, item := range v {
			fmt.Printf("->%s\n", item.Identifier())
		}
		// wouldn't actually delete
		err := BulkAddStagingForDelete(v...)
		if err != nil {
			fmt.Printf("saving error: %v\n", err)
		}
	}
}

/*
type ResourceStasher interface {
	Items() map[string][]Identifiable
	AddItems(string, ...Identifiable)
	StashItems()
}

type resourceStasher struct {
	options StasherOptions
	list    map[string][]Identifiable
}

func (s resourceStasher) Items() map[string][]Identifiable {
	return s.list
}

func NewResourceStasher() ResourceStasher {
	// NOTE: making default big - but also not even
	// utilizing yet
	options := StasherOptions{FlushSize: 1000000}
	stashMap := make(map[string][]Identifiable)
	return &resourceStasher{
		list:    stashMap,
		options: options,
	}
}

func (s resourceStasher) AddItems(typeName string, objs ...Identifiable) {
	// NOTE: might want to add something here to 'BulkAddStaging'
	// if size is > FlushSize (at some point)
	s.list[typeName] = append(s.list[typeName], objs...)
}

func (s resourceStasher) StashItems() {
	for k, v := range s.Items() {
		fmt.Printf("**** %s *****\n", k)
		for _, item := range v {
			fmt.Printf("->%s\n", item.Identifier())
		}
		err := BulkAddResources(k, v...)
		if err != nil {
			fmt.Printf("saving error: %v\n", err)
		}
	}
}

func (s resourceStasher) DeleteItems() {
	for k, v := range s.Items() {
		fmt.Printf("**** %s *****\n", k)
		for _, item := range v {
			fmt.Printf("->%s\n", item.Identifier())
		}
		err := BulkRemoveResources(v...)
		if err != nil {
			fmt.Printf("saving error: %v\n", err)
		}
	}
}

*/
