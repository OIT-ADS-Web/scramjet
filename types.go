package staging_importer

import "fmt"

type UriAddressable interface {
	Uri() string
}

type Identifiable interface {
	Identifier() string
}

type ValidatorFunc func(json string) bool

//https://travix.io/factory-method-pattern-in-go-2e09b233453e
type (
	Stasher interface {
		Items() map[string][]Identifiable
		AddItems(string, ...Identifiable)
		StashItems()
	}

	stasherImpl struct {
		options StasherOptions
		list    map[string][]Identifiable
	}

	// just an idea not sure yet
	StasherOptions struct {
		FlushSize int
	}
)

func (s stasherImpl) Items() map[string][]Identifiable {
	return s.list
}

func NewStasher() Stasher {
	// NOTE: making default big - but also not even
	// utilizing yet
	options := StasherOptions{FlushSize: 1000000}
	stashMap := make(map[string][]Identifiable)
	return &stasherImpl{
		list:    stashMap,
		options: options,
	}
}

func (s stasherImpl) AddItems(typeName string, objs ...Identifiable) {
	// NOTE: might want to add something here to 'BulkAddStaging'
	// if size is > FlushSize (at some point)
	s.list[typeName] = append(s.list[typeName], objs...)
}

func (s stasherImpl) StashItems() {
	for k, v := range s.Items() {
		fmt.Printf("**** %s *****\n", k)
		for _, item := range v {
			fmt.Printf("->%s\n", item.Identifier())
		}
		err := BulkAddStaging(k, v...)
		if err != nil {
			fmt.Printf("saving error: %v\n", err)
		}
	}
}
