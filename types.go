package staging_importer

type UriAddressable interface {
	Uri() string
}

type Identifiable interface {
	Identifier() string
}

type ValidatorFunc func(json string) bool

type UriFunc func(res StagingResource) string
