package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
)

func commandRunCreateSourceFolder(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
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
	home_directory, home_directory_errors := host_user.GetHomeDirectoryAbsoluteDirectory()
	if home_directory_errors != nil {
		errors = append(errors, home_directory_errors...)
	}

	if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	directory_parts := home_directory.GetPath()
	directory_parts = append(directory_parts, "src")

	host_client := processor.GetHostClient()
	sources_folder, sources_folder_errors := host_client.AbsoluteDirectory(directory_parts)

	if sources_folder_errors != nil {
		errors = append(errors, sources_folder_errors...)
	} else {
		create_if_does_not_exist_errors := sources_folder.CreateIfDoesNotExist()
		if create_if_does_not_exist_errors != nil {
			errors = append(errors, create_if_does_not_exist_errors...)
		}
	}

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunCreateSourceFolderFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunCreateSourceFolder
	return &funcValue
}