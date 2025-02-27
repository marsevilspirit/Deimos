package main

import (
	"fmt"
	"log"
	"os"
)

const verbose = true

var logger *log.Logger

func init() {
	logger = log.New(os.Stdout, "[etcd] ", log.Lmicroseconds)
}

func debugf(msg string, v ...any) {
	if verbose {
		logger.Printf("DEBUG "+msg+"\n", v...)
	}
}

func debug(v ...any) {
	if verbose {
		logger.Println("DEBUG " + fmt.Sprint(v...))
	}
}

func warnf(msg string, v ...any) {
	logger.Printf("WARN  "+msg+"\n", v...)
}

func warn(v ...any) {
	logger.Println("WARN " + fmt.Sprint(v...))
}

func fatalf(msg string, v ...any) {
	logger.Printf("FATAL "+msg+"\n", v...)
	os.Exit(1)
}

func fatal(v ...any) {
	logger.Println("FATAL " + fmt.Sprint(v...))
	os.Exit(1)
}
