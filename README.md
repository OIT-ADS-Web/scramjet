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

# as library (API)

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
	pass1 := sj.Packet{Id: sj.Identifier{Id: person1.Id, Type: typeName}, Obj: person1}
	pass2 := sj.Packet{Id: sj.Identifier{Id: person2.Id, Type: typeName}, Obj: person2}

	people := []sj.Storeable{pass1, pass2}
	err := sj.BulkAddStaging(people...)

  // 2) run through a 'validator' function - would likely
  //    be a json schema validator
	alwaysOkay := func(json string) bool { return true }
	valid, rejects, err := sj.FilterTypeStaging("person", alwaysOkay)

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

# as executable (CLI)

Have not done anything with this so far

# General Idea

two tables, `staging` and `resources`

* staging: [id+type=uid]

  actions:
  * stash ->
  * validate -> 
  * stash and validate ->

* resources: [uri(type)=uid]

  actions:
  * move over valid (could be updates)
  * get actual updates (only)

# Operations

## Moving entire 'type' as bulk

```go

  import (
	  sj "github.com/OIT-ADS-Web/scramjet"
  )

	typeName := "person"
  // see above - gather data however it can be gathered
	//err := sj.BulkAddStaging(people...)
  err := sj.StashStaging(people...)
  // own validator function ...
	alwaysOkay := func(json string) bool { return true }
	valid, rejects, err := sj.FilterTypeStaging("person", alwaysOkay)

  err = sj.BatchMarkValidInStaging(valid)
  err = sj.BatchMarkInValidInStaging(rejects)
  
  list, err := sj.RetrieveValidStaging(typeName)
	err = sj.BulkMoveStagingTypeToResources(typeName, list...)


```

## Moving by id (single items)

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
## Moving by query (for instance per person)

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

## More configurable intake

For more advanced use - intake can be run as a series of chunks of input, 
given a listmaker etc... see `ChunkableIntakeConfig`


```
# Basic structure
![image of basic structure](docs/ScramjetBasic.png "A diagram of basic ideas")


# Tables
![image of tables](docs/ScramjetTables.png "A diagram of table structure")