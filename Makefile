SHELL := /bin/bash

all: build

build:
	go build -o bin/dbinjector main.go