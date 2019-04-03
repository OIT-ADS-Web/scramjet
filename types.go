package staging_importer

//https://blog.chewxy.com/2018/03/18/golang-interfaces/
type UriAddressable interface {
	URI() string
}

type ValidatorFunc func(json string) bool
