package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
    "path/filepath"
	"fmt"
)

func commandRunCreateInstanceFolder(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors := validateRunCommandHeaders(processor, request)
	if errors == nil {
		var new_errors []error
		errors = new_errors
	} else if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	host_user := processor.GetHostUser()
	destination_username := processor.CalculateDesintationHostUserName(*branch_instance_id)
	host_client := processor.GetHostClient()
	destination_user, destination_user_errors := host_client.User(destination_username)
	if destination_user_errors != nil {
		errors = append(errors, destination_user_errors...)
	}

	destination_host, destination_host_errors := host_client.Host("127.0.0.1")
	if destination_host_errors != nil {
		errors = append(errors, destination_host_errors...)
	}

	destination_host_user := host_client.HostUser(*destination_host, *destination_user)

	if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	// todo: validate directory names do things depending if branch or tag
	var directory_parts []string
	directory_parts = append(directory_parts, "src")
	directory_parts = append(directory_parts, *domain_name)
	directory_parts = append(directory_parts, *repository_account_name)
	directory_parts = append(directory_parts, *repository_name)
	directory_parts = append(directory_parts, "branch_instances")
	directory_parts = append(directory_parts, fmt.Sprintf("%d", *branch_instance_id))
	full_path_of_directory := "./" + filepath.Join(directory_parts...)

	_, command_errors := host_user.ExecuteRemoteUnsafeCommandUsingFilesWithoutInputFile(destination_host_user, fmt.Sprintf("mkdir -p %s", full_path_of_directory))
	if command_errors != nil {
		errors = append(errors, command_errors...)
	}

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunCreateInstanceFolderFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunCreateInstanceFolder
	return &funcValue
}