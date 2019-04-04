package staging_importer

//https://blog.chewxy.com/2018/03/18/golang-interfaces/
type UriAddressable interface {
	Uri() string
}

type Identifiable interface {
	Identifier() string
}

type ValidatorFunc func(json string) bool
