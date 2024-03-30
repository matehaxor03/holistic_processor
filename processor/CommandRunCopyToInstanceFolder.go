package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
    "path/filepath"
	"fmt"
	"os"
)

func commandRunCopyToInstanceFolder(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
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
	//depending if doing by branch or tag

	host_user := processor.GetHostUser()
	home_directory, home_directory_errors := host_user.GetHomeDirectoryAbsoluteDirectory()
	if home_directory_errors != nil {
		errors = append(errors, home_directory_errors...)
	}

	ssh_directory, ssh_directory_errors := host_user.GetDirectorySSHAbsoluteDirectory()
	if ssh_directory_errors != nil {
		errors = append(errors, ssh_directory_errors...)
	}

	if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	// todo: validate directory names
	directory_parts := home_directory.GetPath()
	directory_parts = append(directory_parts, "src")
	directory_parts = append(directory_parts, *domain_name)
	directory_parts = append(directory_parts, *repository_account_name)
	directory_parts = append(directory_parts, *repository_name)
	directory_parts = append(directory_parts, "branches")
	directory_parts = append(directory_parts, *branch_name)
	directory_parts = append(directory_parts, *repository_name)
	full_path_of_directory := "/" + filepath.Join(directory_parts...)

	var instance_folder_parts []string
	instance_folder_parts = append(instance_folder_parts, "src")
	instance_folder_parts = append(instance_folder_parts, *domain_name)
	instance_folder_parts = append(instance_folder_parts, *repository_account_name)
	instance_folder_parts = append(instance_folder_parts, *repository_name)
	instance_folder_parts = append(instance_folder_parts, "branch_instances")
	instance_folder_parts = append(instance_folder_parts, fmt.Sprintf("%d", *branch_instance_id))
	instance_folder_parts = append(instance_folder_parts, *repository_name)
	full_path_of_instance_directory := "./" + filepath.Join(instance_folder_parts...)

	destination_username := processor.CalculateDesintationHostUserName(*branch_instance_id)
	ssh_identity_file := ssh_directory.GetPathAsString() + "/" + destination_username

	bashCommand := common.NewBashCommand()
	if _, stat_error := os.Stat(full_path_of_directory); !os.IsNotExist(stat_error) {
		copy_command := fmt.Sprintf("scp -i %s -r %s %s:%s", ssh_identity_file, full_path_of_directory, destination_username, full_path_of_instance_directory)
		_, bash_command_errors := bashCommand.ExecuteUnsafeCommandUsingFilesWithoutInputFile(copy_command)
		if bash_command_errors != nil {
			errors = append(errors, bash_command_errors...)
		} 
	} else {
		fmt.Println("does not exist " + full_path_of_directory)
	}

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunCopyToInstanceFolderFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunCopyToInstanceFolder
	return &funcValue
}