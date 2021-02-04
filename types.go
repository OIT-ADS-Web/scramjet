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

type Packet struct {
	Id  Identifier
	Obj interface{} // this will be serialized
}

func (p Packet) Identifier() Identifier {
	return p.Id
}

func (ps Packet) Object() interface{} {
	return ps.Obj
}

type ValidatorFunc func(json string) bool
