#!/bin/sh

cd cmd/staging_importer
go build
cd ../../
cd cmd/scramjet
go build
cd ../../
