package main

import (
	processor "github.com/matehaxor03/holistic_processor/processor"
	"os"
	"fmt"
)

func main() {
	var errors []error
	processor_server, processor_server_errors := processor.NewProcessorServer(nil, nil, "5002", "server.crt", "server.key", "127.0.0.1", "5000")
	if processor_server_errors != nil {
		errors = append(errors, processor_server_errors...)	
	} else {
		processor_server_start_errors := processor_server.Start()
		if processor_server_start_errors != nil {
			errors = append(errors, processor_server_start_errors...)
		}
	}

	if len(errors) > 0 {
		fmt.Println(fmt.Sprintf("%s", errors))
		os.Exit(1)
	}
	
	os.Exit(0)
}


