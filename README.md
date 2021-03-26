# Vivo Scramjet

# module github.com/OIT-ADS-Web/scramjet

A persistent cache of arbitrary json objects associated with 
an id, that can be validated against a service and, when valid,
examined for changes.

This makes it possible to gather some entities for ingest into
a store of some sort - and be able to send only adds, updates or
deletes to that store.

** NOTE ** this is in very early development and likely to change
substantially.  So for the time being, it is here for instructional purposes
only, I would not recommend using it with any projects.  

# General Idea (how it works)

There are two tables, `staging` and `resources`

* staging:
  
  columns:
  * id (text - example "per000001")
  * type (text - example "person")

  Those combined are the *primary key*

  * json (json representation of object)
  * is_valid (if it's been validated)
  * to_delete (if it's passing through as record to delete)

  actions to do:
  * stash -> put stuff in
  * validate -> mark is_valid = (true|false)
  * delete -> stash and to_delete = true

* resources:
  
  columns (from staging):
  * id (text - example "per000001")
  * type (text - example "person")
    
  Those combined are the *primary key*

  * hash (hash of json data)

  This enables a quick determination of whether record has changed
  after importing (whether to change updated_at)

  * data (json representation, from staging)
  * data_b (json representation, from staging - but binary for indexing)

  See `postgresql` documentation about 
  [json_b](https://www.postgresql.org/docs/9.4/datatype-json.html) data type

  * created_at (when record created)
  * updated_at (when record last updated)
		
	NOTE: CONSTRAINT uniq_id_hash UNIQUE (id, type, hash)

  actions:
  * traject -> move over is_valid from staging (could be updates)
  * list -> all, or actual updates etc...
  * delete -> remove to_delete from staging

Once a record has made it to resources, it is removed from staging

# Simplest example

This would be typical usage - bulk importing a type of record. For
example - a nightly csv feed, or nightly table dump - that is a full
set of records each time

```go

import (
	sj "github.com/OIT-ADS-Web/scramjet"
)

...

	typeName := "person"

	// 1. typically would start with database list
	dbList := func() []IntakePerson {
		person1 := IntakePerson{Id: "per0000001", Name: "Test1"}
		person2 := IntakePerson{Id: "per0000002", Name: "Test2"}
		return []IntakePerson{person1, person2}
	}

  // 2. then turn into a list of 'Storeable' objects
	listMaker := func(i int) ([]sj.Storeable, error) {
		var people []sj.Storeable
		for _, person := range dbList() {
			pass := sj.MakePacket(person.Id, typeName, person)
			people = append(people, pass)
		}
		return people, nil
	}

	// 3. create a validator function that validates json representation
	alwaysOkay := func(json string) bool { return true }

  // 4. then construct configs for intake, trajectory (moving from staging to resources)
  //    and finding deletes (records in resources no longer valid)
	intake := sj.IntakeConfig{TypeName: typeName, Count: 2, ChunkSize: 1, ListMaker: listMaker}
	move := sj.TrajectConfig{TypeName: typeName, Validator: alwaysOkay}

	// 5. this would typically be database call for all ids of 'type'
	//     comparing against resources ids of that 'type'
	ids := func() ([]string, error) {
		var ids []string
		for _, person := range dbList() {
			ids = append(ids, person.Id)
		}
		return ids, nil
	}
	outake := sj.OutakeConfig{TypeName: typeName, ListMaker: ids}

	// 6. main function does all 3 actions on data in one sequence
	err := sj.Scramjet(intake, move, outake)

  ...

```

# Other common use cases

## A service to gives updates only

```golang
  ... // per example above
	intake := sj.IntakeConfig{TypeName: typeName, Count: 2, ChunkSize: 1, ListMaker: listMaker}
	move := sj.TrajectConfig{TypeName: typeName, Validator: alwaysOkay}

  // except no 'outtake' (defered until later - since those are not part of this import)
	err := sj.ScramjetIntake(intake, move)
  
  ...
  // then later delete
  outake := sj.OutakeConfig{TypeName: typeName, ListMaker: ids}

	err = sj.ScramjetOutake(outake)


```

## One record at a time (on save/delete)

## A group of records per person


# Controlling each stage of import

It's also possible to do any of those stages individually, if that is more
useful

* Staging Table

```go

import (
	sj "github.com/OIT-ADS-Web/scramjet"
)
...

	typeName := "person"
  // 1) add data
	person1 := TestPerson{Id: "per0000001", Name: "Test1"}
	person2 := TestPerson{Id: "per0000002", Name: "Test2"}
	// must use anything of interface 'Storeable'
  // there is a MakePacket wrapper
	pass1 := sj.Packet{Id: sj.Identifier{Id: person1.Id, Type: typeName}, Obj: person1}
	pass2 := sj.Packet{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}

	people := []sj.Storeable{pass1, pass2}
	err := sj.BulkAddStaging(people...)

  // 2) run through a 'validator' function - would likely
  //    be a json schema validator
	alwaysOkay := func(json string) bool { return true }
	valid, rejects, err := sj.FilterTypeStaging("person", alwaysOkay)

  // 3) can mark them yourself if you want
  err = sj.BatchMarkValidInStaging(valid)
  err = sj.BatchMarkInValidInStaging(rejects)

  // 3) Now the valid ones are marked and ready to go into
  //    'resource' table
    ...


    
```

* Resources Table

```go

import (
	sj "github.com/OIT-ADS-Web/scramjet"
)

...

	typeName := "person"
	list, err := sj.RetrieveValidStaging(typeName)
	err = sj.BulkMoveStagingTypeToResources(typeName, list...)

    ...

```

## Example moving entire 'type' as bulk, in incremental steps

```go

  import (
	  sj "github.com/OIT-ADS-Web/scramjet"
  )

	typeName := "person"
  // see above - gather data however it can be gathered
  err := sj.StashStaging(people...)
  // own validator function ...
	alwaysOkay := func(json string) bool { return true }
	valid, rejects, err := sj.FilterTypeStaging("person", alwaysOkay)

  err = sj.BatchMarkValidInStaging(valid)
  err = sj.BatchMarkInValidInStaging(rejects)
  
  list, err := sj.RetrieveValidStaging(typeName)
	err = sj.BulkMoveStagingTypeToResources(typeName, list...)


```

## Example moving by id (single items)

```go

  import (
	  sj "github.com/OIT-ADS-Web/scramjet"
  )

	typeName := "person"
  // see above - grab single record however necessary
  // and stash in staging table
	err := sj.StashStaging(people...)
  // just need basic 'id' to grab to validate
  identifier := sj.Identifier{Id: id, Type: typeName}
	stub := sj.Stub{Id: identifier}
  // validate however you want
	alwaysOkay := func(json string) bool { return true }
	err = sj.ProcessSingleStaging(stub, alwaysOkay)

  // move it over
  staging, err := sj.RetrieveSingleStagingValid(id, typeName)
	err = sj.BulkMoveStagingTypeToResources(typeName, staging)

```
## Example moving by query (for instance per person)

```go

  import (
	  sj "github.com/OIT-ADS-Web/scramjet"
  )

	typeName := "person"
  // see above - get records of person and stash in staging table
	err := sj.StashStaging(people...)
  // make a validator
	alwaysOkay := func(json string) bool { return true }
	// make a filter - fairly crude on field matcher at this point
  filter := sj.Filter{Field: "externalId", Value: "x200", Compare: sj.Eq}
	// 2. but only get one out
	valid, rejects, err = sj.FilterTypeStagingByQuery(typeName, filter, alwaysOkay)
	err = sj.BatchMarkValidInStaging(valid)
	// move over to resources, based on same filter
  list2, err := sj.RetrieveValidStagingFiltered(typeName, filter)
	err = sj.BulkMoveStagingTypeToResources(typeName, list2...)


```

# Basic structure
![image of basic structure](docs/ScramjetBasic.png "A diagram of basic ideas")


# Tables
![image of tables](docs/ScramjetTables.png "A diagram of table structure")