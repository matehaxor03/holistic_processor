package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
    "path/filepath"
	"fmt"
)

func commandRunClean(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors := validateRunCommandHeaders(request)
	if errors != nil {
		return errors
	} else {
		var new_errors []error
		errors = new_errors
	}

	std_callback := func(message string) {
		fmt.Println(message)
	}

	stderr_callback := func(message error) {
		fmt.Println(message)
	}

	instance_folder_parts := common.GetDataDirectory()
	instance_folder_parts = append(instance_folder_parts, "src")
	instance_folder_parts = append(instance_folder_parts, *domain_name)
	instance_folder_parts = append(instance_folder_parts, *repository_account_name)
	instance_folder_parts = append(instance_folder_parts, *repository_name)
	instance_folder_parts = append(instance_folder_parts, "branch_instances")
	instance_folder_parts = append(instance_folder_parts, fmt.Sprintf("%d", *build_branch_instance_id))
	instance_folder_parts = append(instance_folder_parts, *repository_name)
	full_path_of_instance_directory := "/" + filepath.Join(instance_folder_parts...)

	bashCommand := common.NewBashCommand()
	command := fmt.Sprintf("cd %s && go clean", full_path_of_instance_directory)
	_, bash_command_errors := bashCommand.ExecuteUnsafeCommand(command, &std_callback, &stderr_callback)
	if bash_command_errors != nil {
		errors = append(errors, bash_command_errors...)
	} 

	if len(errors) > 0 {
		return errors
	}

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, parameters, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunCleanFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunClean
	return &funcValue
}