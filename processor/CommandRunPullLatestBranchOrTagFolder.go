package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
    "path/filepath"
	"fmt"
	"strings"
)

func commandRunPullLatestBranchOrTagFolder(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors := validateRunCommandHeaders(processor, request)
	if errors == nil {
		var new_errors []error
		errors = new_errors
	} else if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	// todo: validate directory names
	directory_parts := common.GetDataDirectory()
	directory_parts = append(directory_parts, "src")
	directory_parts = append(directory_parts, *domain_name)
	directory_parts = append(directory_parts, *repository_account_name)
	directory_parts = append(directory_parts, *repository_name)
	directory_parts = append(directory_parts, "branches")
	directory_parts = append(directory_parts, *branch_name)
	directory_parts = append(directory_parts, *repository_name)

	full_path_of_directory := filepath.Join(directory_parts...)

	bashCommand := common.NewBashCommand()
	command := fmt.Sprintf("cd %s && git pull", "/" + full_path_of_directory)
	_, bash_command_errors := bashCommand.ExecuteUnsafeCommandUsingFilesWithoutInputFile(command)
	if bash_command_errors != nil {
		for _, error_message := range bash_command_errors {
			if !(strings.Contains(fmt.Sprintf("%s", error_message), "-> ") ||
				strings.Contains(fmt.Sprintf("%s", error_message), "file changed") ||
				strings.Contains(fmt.Sprintf("%s", error_message), "Updating") ||
				strings.Contains(fmt.Sprintf("%s", error_message), "From ") ||
				strings.Contains(fmt.Sprintf("%s", error_message), "Fast-forward") ||
				strings.Contains(fmt.Sprintf("%s", error_message), ".go ")) {
			errors = append(errors, error_message)
			}
		}
	} 

	

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunPullLatestBranchOrTagFolderFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunPullLatestBranchOrTagFolder
	return &funcValue
}