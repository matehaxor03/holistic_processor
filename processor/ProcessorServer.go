package processor

import (
	"net/http"
	"fmt"
	"strings"
	"strconv"
	"sync"
	common "github.com/matehaxor03/holistic_common/common"
	dao "github.com/matehaxor03/holistic_db_client/dao"
	json "github.com/matehaxor03/holistic_json/json"
	validate "github.com/matehaxor03/holistic_validator/validate"
)

type ProcessorServer struct {
	Start   func() ([]error)
	SetQueueCompleteFunctions func(map[string](*func(json.Map) []error))
	GetQueueCompleteFunction func(queue string) (*func(json.Map) []error)
	SetQueuePushBackFunctions func(map[string](*func(json.Map) (*json.Map, []error)))
	GetQueuePushBackFunction func(queue string) (*func(json.Map) (*json.Map, []error))
	SetQueuePushFrontFunctions func(map[string](*func(json.Map) (*json.Map, []error)))
	GetQueuePushFrontFunction func(queue string) (*func(json.Map) (*json.Map, []error))
	SetQueueGetNextMessageFunctions func(map[string](*func(string) (json.Map, []error)))
	GetQueueGetNextMessageFunction func(queue string) (*func(string) (json.Map, []error))
	GetControllers func() (map[string](*ProcessorController))
	GetControllerByName func(controller_name string) (*ProcessorController, error)
	GetControllerNames func() []string
}

func NewProcessorServer(port string, server_crt_path string, server_key_path string, queue_domain_name string, queue_port string) (*ProcessorServer, []error) {
	var errors []error
	var this_processor_server *ProcessorServer
	verify := validate.NewValidator()
	get_controller_by_name_lock := &sync.RWMutex{}
	
	get_queue_complete_lock := &sync.RWMutex{}
	get_queue_push_back_lock := &sync.RWMutex{}
	get_queue_push_front_lock := &sync.RWMutex{}
	get_queue_get_next_message_lock := &sync.RWMutex{}

	var queue_complete_functions map[string](*func(json.Map) []error)
	var queue_push_back_functions map[string](*func(json.Map) (*json.Map, []error))
	var queue_push_front_functions map[string](*func(json.Map) (*json.Map, []error))
	var queue_get_next_message_functions map[string](*func(string) (json.Map, []error))

	set_queue_complete_functions := func(functions map[string](*func(json.Map) []error)) {
		queue_complete_functions = functions
	}

	get_queue_complete_functions := func() map[string](*func(json.Map) []error) {
		return queue_complete_functions
	}

	set_queue_push_back_functions := func(functions map[string](*func(json.Map) (*json.Map, []error))) {
		queue_push_back_functions = functions
	}

	get_queue_push_back_functions := func() map[string](*func(json.Map) (*json.Map, []error)) {
		return queue_push_back_functions
	}

	set_queue_push_front_functions := func(functions map[string](*func(json.Map) (*json.Map, []error))) {
		queue_push_front_functions = functions
	}

	get_queue_push_front_functions := func() map[string](*func(json.Map) (*json.Map, []error)) {
		return queue_push_front_functions
	}

	set_queue_get_next_message_functions := func(functions map[string](*func(string) (json.Map, []error))) {
		queue_get_next_message_functions = functions
	}

	get_queue_get_next_message_functions := func() map[string](*func(string) (json.Map, []error)) {
		return queue_get_next_message_functions
	}

	client_manager, client_manager_errors := dao.NewClientManager()
	if client_manager_errors != nil {
		return nil, client_manager_errors
	}
	
	test_read_client, test_read_client_errors := client_manager.GetClient("127.0.0.1", "3306", "holistic", "holistic_r")
	if test_read_client_errors != nil {
		return nil, test_read_client_errors
	}

	set_processor_server := func(processor_server *ProcessorServer) {
		this_processor_server = processor_server
	}

	get_processor_server := func() *ProcessorServer {
		return this_processor_server
	}
	
	test_read_database := test_read_client.GetDatabase()
	
	controllers := make(map[string](*ProcessorController))
	
	table_names, table_names_errors := test_read_database.GetTableNames()
	if table_names_errors != nil {
		return nil, table_names_errors
	}

	domain_name, domain_name_errors := dao.NewDomainName(verify, queue_domain_name)
	if domain_name_errors != nil {
		return nil, domain_name_errors
	}

	getPort := func() string{
		return port
	}

	getServerCrtPath := func() string {
		return server_crt_path
	}

	getServerKeyPath := func() string {
		return server_key_path
	}

	validate := func() []error {
		return nil
	}

	for _, table_name := range table_names {
		create_processor, create_processor_errors := NewProcessorController(*verify, client_manager, *domain_name, queue_port, "CreateRecords_" + table_name, 1, -1)
		if create_processor_errors != nil {
			errors = append(errors, create_processor_errors...)
		} else if create_processor != nil {
			controllers["CreateRecords_" + table_name] = create_processor
		}

		get_table_count_processor, get_table_count_processor_errors := NewProcessorController(*verify, client_manager, *domain_name, queue_port, "GetTableCount_" + table_name, 1, -1)
		if get_table_count_processor_errors != nil {
			errors = append(errors, get_table_count_processor_errors...)
		} else if get_table_count_processor != nil {
			controllers["GetTableCount_" + table_name] = get_table_count_processor
		}

		create_record_processor, create_record_processor_errors := NewProcessorController(*verify, client_manager, *domain_name, queue_port, "CreateRecord_" + table_name, 1, -1)
		if create_record_processor_errors != nil {
			errors = append(errors, create_record_processor_errors...)
		} else if create_record_processor != nil {
			controllers["CreateRecord_" + table_name] = create_record_processor
		}

		read_processor, read_processor_errors := NewProcessorController(*verify, client_manager, *domain_name, queue_port, "ReadRecords_" + table_name, 1, -1)
		if read_processor_errors != nil {
			errors = append(errors, read_processor_errors...)
		} else if read_processor != nil {
			controllers["ReadRecords_" + table_name] = read_processor
		}

		update_processor, update_processor_errors := NewProcessorController(*verify, client_manager, *domain_name, queue_port, "UpdateRecords_" + table_name, 1, -1)
		if update_processor_errors != nil {
			errors = append(errors, update_processor_errors...)
		} else if update_processor != nil {
			controllers["UpdateRecords_" + table_name] = update_processor
		}

		update_record_processor, update_record_processor_errors := NewProcessorController(*verify, client_manager, *domain_name, queue_port, "UpdateRecord_" + table_name, 1, -1)
		if update_record_processor_errors != nil {
			errors = append(errors, update_record_processor_errors...)
		} else if update_record_processor != nil {
			controllers["UpdateRecord_" + table_name] = update_record_processor
		}

		get_schema_processor, get_schema_processor_errors := NewProcessorController(*verify, client_manager, *domain_name, queue_port, "GetSchema_" + table_name, 1, -1)
		if get_schema_processor_errors != nil {
			errors = append(errors, get_schema_processor_errors...)
		} else if get_schema_processor != nil {
			controllers["GetSchema_" + table_name] = get_schema_processor
		}
	}

	commands_list := [...]string{"Run_Sync:1:1",
								"Run_StartBranchInstance:1:1", 
								"Run_NotStarted:1:1", 
								"Run_Start:1:1",
								"Run_CreateSourceFolder:1:1",
								"Run_CreateDomainNameFolder:1:-1",
								"Run_CreateRepositoryAccountFolder:1:1",
								"Run_CreateRepositoryFolder:1:1",
								"Run_CreateBranchesFolder:1:1",
								"Run_CreateTagsFolder:1:1",
								"Run_CreateBranchOrTagFolder:1:1",
								"Run_CloneBranchOrTagFolder:1:1",
								"Run_PullLatestBranchOrTagFolder:1:1",
								"Run_CreateBranchInstancesFolder:1:1",
								"Run_CreateTagInstancesFolder:1:1",
								"Run_CopyToInstanceFolder:1:1",
								"Run_CreateInstanceFolder:1:1",
								"Run_CreateGroup:1:1",
								"Run_CreateUser:1:1",
								"Run_AssignGroupToUser:1:1",
								"Run_AssignGroupToInstanceFolder:1:1",
								"Run_Clean:1:-1",
								"Run_Lint:1:-1",
								"Run_Build:1:-1",
								"Run_UnitTests:1:-1",
								"Run_IntegrationTests:1:-1",
								"Run_IntegrationTestSuite:1:-1",
								"Run_RemoveGroupFromInstanceFolder:1:1",
								"Run_RemoveGroupFromUser:1:1",
								"Run_DeleteGroup:1:1",
								"Run_DeleteUser:1:1",
								"Run_DeleteInstanceFolder:1:1",
								"Run_End:1:1",
								"GetTableNames:1:1"}


	for _, command := range commands_list {
		parts := strings.Split(command, ":")
		if len(parts) != 3 {
			errors = append(errors, fmt.Errorf("parts is not 3 for processor manager: " + command))
			continue
		}
		
		command_name := parts[0]

		minimum_threads, minimum_threads_error := strconv.ParseInt(string(parts[1]), 10, 64)
		if minimum_threads_error != nil {
			errors = append(errors, minimum_threads_error)
			continue
		}

		maximum_threads, maximum_threads_error := strconv.ParseInt(string(parts[2]), 10, 64)
		if maximum_threads_error != nil {
			errors = append(errors, maximum_threads_error)
			continue
		}

		processor, processor_errors := NewProcessorController(*verify, client_manager, *domain_name, queue_port, command_name, int(minimum_threads), int(maximum_threads))
		if processor_errors != nil {
			errors = append(errors, processor_errors...)
		} else if processor != nil {
			controllers[command_name] = processor
		} else {
			errors = append(errors, fmt.Errorf("failed to create processor: %s", command_name))
		}
	}

	get_controller_by_name := func(contoller_name string) (*ProcessorController, error) {
		get_controller_by_name_lock.Lock()
		defer get_controller_by_name_lock.Unlock()
		queue_obj, queue_found := controllers[contoller_name]
		if !queue_found {
			return nil, fmt.Errorf("queue not found %s", contoller_name)
		} else if common.IsNil(queue_obj) {
			return nil, fmt.Errorf("queue found but is nil %s", contoller_name)
		} 
		return queue_obj, nil
	}
	
	get_controller_names := func() []string {
		get_controller_by_name_lock.Lock()
		defer get_controller_by_name_lock.Unlock()
		controller_names := make([]string, len(controllers))
		for key, _ := range controllers {
			controller_names = append(controller_names, key)
		}
		return controller_names
	}

	x := ProcessorServer{
		SetQueueCompleteFunctions: func(functions map[string](*func(json.Map) []error)) {
			set_queue_complete_functions(functions)
		},
		GetQueueCompleteFunction: func(queue_name string) (*func(json.Map) []error) {
			get_queue_complete_lock.Lock()
			defer get_queue_complete_lock.Unlock()
			
			functions := get_queue_complete_functions()
			if functions == nil {
				return nil
			}
			function, found := functions[queue_name]
			if !found {
				return nil
			}
			return function
 		},
		SetQueuePushBackFunctions: func(functions map[string](*func(json.Map) (*json.Map, []error))) {
			set_queue_push_back_functions(functions)
		},
		GetQueuePushBackFunction: func(queue_name string) (*func(json.Map) (*json.Map, []error)) {
			get_queue_push_back_lock.Lock()
			defer get_queue_push_back_lock.Unlock()

			functions := get_queue_push_back_functions()
			if functions == nil {
				return nil
			}
			function, found := functions[queue_name]
			if !found {
				return nil
			}
			return function
		},
		SetQueuePushFrontFunctions: func(functions map[string](*func(json.Map) (*json.Map, []error))) {
			set_queue_push_front_functions(functions)
		},
		GetQueuePushFrontFunction: func(queue_name string) (*func(json.Map) (*json.Map, []error)) {
			get_queue_push_front_lock.Lock()
			defer get_queue_push_front_lock.Unlock()

			functions := get_queue_push_front_functions()
			if functions == nil {
				return nil
			}
			function, found := functions[queue_name]
			if !found {
				return nil
			}
			return function
		},
		SetQueueGetNextMessageFunctions: func(functions map[string](*func(string) (json.Map, []error))) {
			set_queue_get_next_message_functions(functions)
		},
		GetQueueGetNextMessageFunction: func(queue_name string) (*func(string) (json.Map, []error)) {
			get_queue_get_next_message_lock.Lock()
			defer get_queue_get_next_message_lock.Unlock()
			functions := get_queue_get_next_message_functions()
			if functions == nil {
				return nil
			}
			function, found := functions[queue_name]
			if !found {
				return nil
			}
			return function
		},
		GetControllerByName: func(controller_name string) (*ProcessorController, error) {
			return get_controller_by_name(controller_name)
		},
		GetControllerNames: func() []string {
			return get_controller_names()
		},
		GetControllers: func() (map[string](*ProcessorController)) {
			return controllers
		},
		Start: func() []error {
			var start_server_errors []error

			temp_controller_names := get_controller_names()

			for _, temp_controller_name := range temp_controller_names {
				temp_controller, temp_controller_errors := get_controller_by_name(temp_controller_name)
				if temp_controller_errors != nil {
					start_server_errors = append(start_server_errors, temp_controller_errors)
					continue
				} else if common.IsNil(temp_controller) {
					start_server_errors = append(start_server_errors, fmt.Errorf("controller is nil: %s", temp_controller))
					continue
				}
				temp_controller.SetProcessorServer(get_processor_server())
				http.HandleFunc("/processor_api/" + temp_controller_name, *(temp_controller.GetProcessRequestFunction()))
				temp_controller.Start()
			}

			err := http.ListenAndServeTLS(":"+ getPort(),  getServerCrtPath(), getServerKeyPath(), nil)
			if err != nil {
				start_server_errors = append(start_server_errors, err)
			}

			if len(start_server_errors) > 0 {
				return start_server_errors
			}

			return nil
		},
	}
	set_processor_server(&x)

	validate_errors := validate()
	if validate_errors != nil {
		errors = append(errors, validate_errors...)
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &x, nil
}
