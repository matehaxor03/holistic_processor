package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
)

func commandRunCreateTagInstancesFolder(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
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
	host_client := processor.GetHostClient()
	destination_host_username := processor.CalculateDesintationHostUserName(*branch_instance_id)

	destination_host, destination_host_errors := host_client.Host("127.0.0.1")
	if destination_host_errors != nil {
		errors = append(errors, destination_host_errors...)
	}

	destination_user, destination_user_errors := host_client.User(destination_host_username)
	if destination_user_errors != nil {
		errors = append(errors, destination_user_errors...)
	}

	if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	destination_host_user := host_client.HostUser(*destination_host, *destination_user)

	destination_host_home_directory, destination_host_home_directory_errors := destination_user.GetHomeDirectoryAbsoluteDirectory()
	if destination_host_home_directory_errors != nil {
		errors = append(errors, destination_host_home_directory_errors...)
	}

	if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	directory_parts := destination_host_home_directory.GetPath()
	directory_parts = append(directory_parts, "tag_instances")
	
	remote_absolute_directory, remote_absolute_directory_errors := host_user.RemoteAbsoluteDirectory(destination_host_user, directory_parts)
	if remote_absolute_directory_errors != nil {
		errors = append(errors, remote_absolute_directory_errors...)
	}

	if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	create_errors := remote_absolute_directory.CreateIfDoesNotExist()
	if create_errors != nil {
		errors = append(errors, create_errors...) 
	}

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunCreateTagInstancesFolderFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunCreateTagInstancesFolder
	return &funcValue
}