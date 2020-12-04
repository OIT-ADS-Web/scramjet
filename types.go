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

/*
type Parcel struct {
	//Id  Identifiable
	Id  Identifier
	Obj interface{} // this will be serialized
}

func (p Parcel) Identifier() Identifiable {
	return p.Id
}

func (p Parcel) Object() interface{} {
	return p.Obj
}
*/

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

/*
type Resource struct {
	Id  Identifier
	Obj interface{} // this will be serialized
}

func (rs Resource) Identifier() Identifier {
	return rs.Id
}

func (res Resource) Object() interface{} {
	return res.Obj
}
*/

type ValidatorFunc func(json string) bool
