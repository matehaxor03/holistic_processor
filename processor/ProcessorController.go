package processor

import (
	"net/http"
	"fmt"
	"io/ioutil"
	"sync"
	common "github.com/matehaxor03/holistic_common/common"
	dao "github.com/matehaxor03/holistic_db_client/dao"
	json "github.com/matehaxor03/holistic_json/json"
	http_extension "github.com/matehaxor03/holistic_http/http_extension"
)

type ProcessorController struct {
	GetWakeupProcessorManagerFunction func() *func()
	GetProcessRequestFunction func() *func(w http.ResponseWriter, req *http.Request)
	GetProcessorServer func() *ProcessorServer
	SetProcessorServer func(processor_server *ProcessorServer)
	Start func() []error
}

func NewProcessorController(client_manager *dao.ClientManager, queue_domain_name dao.DomainName, queue_port string, queue_name string, minimum_threads int, maximum_threads int) (*ProcessorController, []error) {
	var errors []error
	var this_processsor_controller *ProcessorController
	var processor_server *ProcessorServer
	started := false
	wakeup_processor_lock := &sync.RWMutex{}
	
	processor_manager, processor_manager_errors := NewProcessorManager(client_manager, queue_domain_name, queue_port, queue_name, minimum_threads, maximum_threads)
	if processor_manager_errors != nil {
		errors = append(errors, processor_manager_errors...)
	} else if common.IsNil(processor_manager) {
		errors = append(errors, fmt.Errorf("failed to create processor: %s", queue_name))
	} 

	if len(errors) > 0 {
		return nil, errors
	}

	validate := func() []error {
		return nil
	}

	set_processor_controller := func(processsor_controller *ProcessorController) {
		this_processsor_controller = processsor_controller
	}

	get_processor_controller := func() *ProcessorController {
		return this_processsor_controller
	}

	get_processor_manager := func() *ProcessorManager {
		return processor_manager
	}

	get_queue_name := func() string {
		return queue_name
	}

	wakeup_processor_manager_function := func() {
		wakeup_processor_lock.Lock()
		defer wakeup_processor_lock.Unlock()
		processor_manager.WakeUp()
	}

	getProcessorServer := func() *ProcessorServer {
		return processor_server
	}

	setProcessorServer := func(value *ProcessorServer)  {
		processor_server = value
	}

	process_request_function := func(w http.ResponseWriter, req *http.Request) {
		var errors []error

		if !(req.Method == "POST" || req.Method == "PATCH" || req.Method == "PUT") {
			errors = append(errors, fmt.Errorf("request method not supported: " + req.Method))
		}

		if len(errors) > 0 {
			http_extension.WriteResponse(w, json.NewMap(), errors)
			return
		}

		body_payload, body_payload_error := ioutil.ReadAll(req.Body);
		if body_payload_error != nil {
			errors = append(errors, body_payload_error)
		} 

		if len(errors) > 0 {
			http_extension.WriteResponse(w, json.NewMap(), errors)
			return
		}
		
		json_payload, json_payload_errors := json.Parse(string(body_payload))
		if json_payload_errors != nil {
			errors = append(errors, json_payload_errors...)
		} 

		if len(errors) > 0 {
			http_extension.WriteResponse(w, json.NewMap(), errors)
			return
		}

		queue_mode, queue_mode_errors := json_payload.GetString("[queue_mode]")

		if queue_mode_errors != nil {
			errors = append(errors, queue_mode_errors...)
		} 
		
		if common.IsNil(queue_mode) {
			errors = append(errors, fmt.Errorf("[queue_mode] is nil"))
		}

		if len(errors) > 0 {
			http_extension.WriteResponse(w, json_payload, errors)
			return
		}

		var result *json.Map

		if *queue_mode == "WakeUp" {
			wakeup_processor_manager_function()
		} else {
			errors = append(errors, fmt.Errorf("[queue_mode] is not supported: %s",  *queue_mode))
		}

		http_extension.WriteResponse(w, result, errors)
	}

	x := ProcessorController{
		GetWakeupProcessorManagerFunction: func() *func() {
			function := wakeup_processor_manager_function
			return &function
		},
		GetProcessRequestFunction: func() *func(w http.ResponseWriter, req *http.Request) {
			function := process_request_function
			return &function
		},
		Start: func() []error {
			var errors []error
			if started {
				errors = append(errors, fmt.Errorf("processor already started %s", get_queue_name()))
				return errors
			}
			get_processor_manager().SetProcessorController(get_processor_controller())
			get_processor_manager().Start()
			started = true
			return nil
		},
		GetProcessorServer: func() *ProcessorServer {
			return getProcessorServer()
		},
		SetProcessorServer: func(processor_server *ProcessorServer) {
			setProcessorServer(processor_server)
		},
	}
	set_processor_controller(&x)

	validate_errors := validate()
	if validate_errors != nil {
		errors = append(errors, validate_errors...)
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &x, nil
}
