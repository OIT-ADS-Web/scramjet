package staging_importer

type UriAddressable interface {
	Uri() string
}

type Identifiable interface {
	Identifier() string
}

type ValidatorFunc func(json string) bool

// function to get URI from ID
type UriFunc func(res StagingResource) string

// function to get the Id from URI
type IdFunc func(res Resource) string
