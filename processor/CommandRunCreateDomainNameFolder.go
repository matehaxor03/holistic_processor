package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"os"
    "path/filepath"
	"fmt"
)

func commandRunCreateDomainNameFolder(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, errors := validateRunCommandHeaders(request)
	if errors != nil {
		return errors
	} else {
		var new_errors []error
		errors = new_errors
	}

	// todo: validate directory names
	directory_parts := common.GetDataDirectory()
	directory_parts = append(directory_parts, "src")
	directory_parts = append(directory_parts, *domain_name)
	
	permissions := int(0700)
	full_path_of_directory := filepath.Join(directory_parts...)
	fmt.Println(full_path_of_directory)
	create_directory_error := os.MkdirAll("/" + full_path_of_directory, os.FileMode(permissions))
	if create_directory_error != nil {
		errors = append(errors, create_directory_error)
	}

	if len(errors) > 0 {
		return errors
	}

	trigger_next_run_command_errors := triggerNextRunCommand(processor, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunCreateDomainNameFolderFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunCreateDomainNameFolder
	return &funcValue
}