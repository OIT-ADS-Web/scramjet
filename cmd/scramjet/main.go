package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/mux"
	"github.com/namsral/flag"
	sj "gitlab.oit.duke.edu/scholars/staging_importer"
)

func HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// TODO: way to check db connection?
	io.WriteString(w, `{"alive": true}`)
}

// or id?
/*

curl -H "Content-Type: application/json" --data @<file-name>.json

curl --header "Content-Type: application/json" \
  --request POST \
  --data '[{"id": "0000001", "name": "Rob"}]' \
  http://localhost:8855/intake/<person>

	/intake/[type]
	{id: "0000001", 'type': 'Person', data: { "id": "0000001", "name": "Rob"} },
	{id: "0000002", 'type': 'Person', data: { "id": "0000002", "name": "Robert"} }, etc...

	POST /intake/person ...
	{"id": "0000001", "name": "Rob"},

	updates (e.g. if we went in {"id": "000001", { "name": "Robb" } }?

	UPDATE objects
    SET body = jsonb_set(body, '{name}', '"Robb"', true)
    WHERE id = 1;

	deletes ...

	r.HandleFunc("/books/{title}", CreateBook).Methods("POST")
r.HandleFunc("/books/{title}", ReadBook).Methods("GET")
r.HandleFunc("/books/{title}", UpdateBook).Methods("PUT")
r.HandleFunc("/books/{title}", DeleteBook).Methods("DELETE")

curl -X "DELETE" http://www.example.com/page

SendOff

/sendoff/<type, id >.("Delete")

conversion - needs multiple passes at <delete>

rdf  solr etc...
[x]  [x]

delete person -> what about educations etc...
if conversion involves conglomerate
need a way to backtrack up
orphans


[Unit]
Nozzle
  --> publication -> associated with (1-many) -> person
  --> education -> associated with 1-1 -> person
  --> authorship -> assoicated with 1-1 -> person


  spindles?
  foreign_key-> cascade deletes

--> Kinetic

TODO: haven't used this yet - probably doesn't work
*/
func IntakeHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	io.WriteString(w, fmt.Sprintf(`{"category": %s}`, vars["category"]))

	var arr []sj.Passenger
	receivedJSON, err := ioutil.ReadAll(r.Body) //This reads raw request body
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal([]byte(receivedJSON), &arr)
	if err != nil {
		panic(err)
	}

	resources := []sj.Storeable{}
	for _, res := range arr {
		// NOTE: category might be 'type' in object already
		res.Id = sj.Identifier{Id: res.Identifier().Id, Type: vars["category"]}
		resources = append(resources, res)
	}
	err = sj.BulkAddStaging(resources...)
	if err != nil {
		panic(err)
	}
}

func TransferHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_, ok := vars["id"]
	if !ok {
		//no id sent
		log.Println("no id specified")
	}

	io.WriteString(w, fmt.Sprintf(`{"category": %s}`, vars["category"]))
}

func LaunchHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// <all>|updates|adds|deletes
	_, ok := vars["group"]
	if !ok {
		//no id sent
		log.Println("no group specified")
	}

	io.WriteString(w, fmt.Sprintf(`{"category": %s}`, vars["category"]))
}

func main() {
	var conf sj.Config
	//var wait time.Duration

	dbServer := flag.String("DB_SERVER", "", "database server")
	dbPort := flag.Int("DB_PORT", 0, "database port")
	dbDatabase := flag.String("DB_DATABASE", "", "database database")
	dbUser := flag.String("DB_USER", "", "database user")
	dbPassword := flag.String("DB_PASSWORD", "", "database password")
	dbMaxConnections := flag.Int("DB_MAX_CONNECTIONS", 1, "database maximum pool conections")
	dbAquireTimeout := flag.Int("DB_ACQUIRE_TIMEOUT", 30, "how many seconds to wait to get connection")
	//wait := flag.Int("graceful-timeout", time.Second * 15,
	//"the duration for which the server gracefully wait for existing connections to finish - e.g. 15s or 1m")
	wait := time.Second * 15 // FIXME: make this configurable?

	flag.Parse()

	if len(*dbServer) == 0 && len(*dbUser) == 0 {
		log.Fatal("database credentials need to be set")
	} else {
		database := sj.DatabaseInfo{
			Server:         *dbServer,
			Database:       *dbDatabase,
			Password:       *dbPassword,
			Port:           *dbPort,
			User:           *dbUser,
			MaxConnections: *dbMaxConnections,
			AcquireTimeout: *dbAquireTimeout,
		}
		conf = sj.Config{
			Database: database,
		}
	}

	if err := sj.MakeConnectionPool(conf); err != nil {
		fmt.Printf("could not establish postgresql connection %s\n", err)
		os.Exit(1)
	}

	if !sj.StagingTableExists() {
		fmt.Println("staging table not found")
		sj.MakeStagingSchema()
	}
	if !sj.ResourceTableExists() {
		fmt.Println("resources table not found")
		sj.MakeResourceSchema()
	}

	// server goes here ...
	router := mux.NewRouter()
	router.HandleFunc("/", HealthCheckHandler)

	/*
				** INTAKE

				<passenger>
				/intake/[type]
				{'type': 'Person': id: [an id]: data: [json] },
				{'type': 'Person': id: [an id]: data: [json] }, etc...

				** TRANSFER

				<wave> (triggered)
				/transfer/[type]/<>
				{'id': callback -->? validate --> }

				** LAUNCH

				<nozzle> (pull?)
				/launch/[type]/(all|changes-only|adds-only)
				{'id' -> changes -- converter -- >}

		       id param?
	*/
	router.HandleFunc("/intake", IntakeHandler).Methods("POST")
	//router.HandleFunc("/intake/{category}", IntakeHandler).Methods("POST")
	router.HandleFunc("/transfer/{category}", TransferHandler).Methods("POST")
	router.HandleFunc("/transfer/{category}/{id:[0-9]+}", TransferHandler).Methods("POST")
	router.HandleFunc("/launch/{category}", LaunchHandler).Methods("GET")
	router.HandleFunc("/launch/{category}/{group}", LaunchHandler).Methods("GET")

	srv := &http.Server{
		Addr: ":8855",
		// Good practice to set timeouts to avoid Slowloris attacks.
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      router, // Pass our instance of gorilla/mux in.
	}
	logger := log.New(os.Stdout, "[scramjet]", log.LstdFlags)
	ctx := context.Background()
	/*
		cancellable, cancel := context.WithCancel(ctx)
		// TODO: is this the correct usage of context cancel?
		// https://dave.cheney.net/2017/08/20/context-isnt-for-cancellation

		// TODO: not sure this is ever actually called
		defer func() {
			cancel()
			if err := srv.Shutdown(cancellable); err != nil {
				logger.Fatalf("could not shutdown: %v", err)
			}
			logger.Println("shutting down")
			os.Exit(1)
		}()
	*/
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			logger.Fatalf("could not start server: %v", err)
			os.Exit(1)
		}
	}()

	c := make(chan os.Signal, 1)
	// We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C)
	// SIGKILL, SIGQUIT or SIGTERM (Ctrl+/) will not be caught.
	signal.Notify(c, os.Interrupt)

	// Block until we receive our signal.
	<-c
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	// Doesn't block if no connections, but will otherwise wait
	// until the timeout deadline.
	srv.Shutdown(ctx)
	// Optionally, you could run srv.Shutdown in a goroutine and block on
	// <-ctx.Done() if your application should wait for other services
	// to finalize based on context cancellation.
	log.Println("shutting down")
	os.Exit(0)

	defer sj.DBPool.Close()
}
