package staging_importer

type Identifier struct {
	Id, Type string
}

type Identifiable interface {
	Identifier() Identifier
}

type Storeable interface {
	Identifier() Identifier
	Object() interface{}
}

type Stub struct {
	Id Identifier
}

func (s Stub) Identifier() Identifier {
	return s.Id
}

type Passenger struct {
	Id  Identifier
	Obj interface{} // this will be serialized
}

func (ps Passenger) Identifier() Identifier {
	return ps.Id
}

func (ps Passenger) Object() interface{} {
	return ps.Obj
}

type ValidatorFunc func(json string) bool
