package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	engine "github.com/OpenNSW/go-temporal-workflow"
)

// TaskManager orchestrates external task requests and mock completion
type TaskManager struct {
	db            *TaskDB
	layer1Manager engine.TemporalManager
	layer2Manager engine.TemporalManager
}

func NewTaskManager(db *TaskDB, layer1 engine.TemporalManager, layer2 engine.TemporalManager) *TaskManager {
	return &TaskManager{
		db:            db,
		layer1Manager: layer1,
		layer2Manager: layer2,
	}
}

// StartTask handles an incoming task request from the workflow engine (Layer 1).
// It starts a Layer 2 workflow based on task.json.
func (tm *TaskManager) StartTask(payload engine.TaskPayload) error {
	taskID := payload.NodeID // using NodeID as unique identifier for this run
	layer2WorkflowID := fmt.Sprintf("task-layer2-%s-%s", payload.WorkflowID, payload.NodeID)

	// 1. Write task information to DB
	record := TaskRecord{
		WorkflowID:       payload.WorkflowID,
		RunID:            payload.RunID,
		NodeID:           payload.NodeID,
		TaskTemplateID:   payload.TaskTemplateID,
		Layer2WorkflowID: layer2WorkflowID,
		Status:           "PENDING_L2",
		CreatedAt:        time.Now(),
	}
	tm.db.SaveTask(taskID, record)
	log.Printf("[TaskManager] Persisted new task to DB. TaskID: %s, L2 Workflow: %s", taskID, layer2WorkflowID)

	// 2. Load task.json
	fileBytes, err := os.ReadFile("task.json")
	if err != nil {
		return fmt.Errorf("failed to read task.json: %v", err)
	}

	var def engine.WorkflowDefinition
	if err := json.Unmarshal(fileBytes, &def); err != nil {
		return fmt.Errorf("failed to parse task.json: %v", err)
	}

	// 3. Start Layer 2 Workflow
	log.Printf("[TaskManager] starting layer 2 workflow with payload %v", payload)
	
	initialVars := map[string]any{
		"applicant_name": "",
		"justification": "",
		"reviewer_comments": "",
		"review_outcome": "",
	}
	
	err = tm.layer2Manager.StartWorkflow(context.Background(), layer2WorkflowID, def, initialVars)
	if err != nil {
		return fmt.Errorf("failed to start layer 2 workflow: %v", err)
	}

	log.Printf("[TaskManager] Successfully started Layer 2 workflow: %s", layer2WorkflowID)

	return nil
}

// HandleLayer2Completion is called when ANY workflow completes.
// We check if it's a Layer 2 workflow, and if so, complete the corresponding Layer 1 task.
func (tm *TaskManager) HandleLayer2Completion(workflowID string, finalVariables map[string]any) error {
	record, exists := tm.db.GetTaskByLayer2WorkflowID(workflowID)
	if !exists {
		// Not a layer 2 workflow we launched, or DB lost it. Safe to ignore.
		return nil
	}

	log.Printf("[TaskManager] Detected completion of Layer 2 workflow %s for Task %s", workflowID, record.NodeID)

	// Update DB status and store the final outcome
	record.Status = "COMPLETED"
	record.Inputs = finalVariables
	tm.db.SaveTask(record.NodeID, record)

	// Complete the Layer 1 task in Temporal
	err := tm.layer1Manager.TaskDone(context.Background(), record.WorkflowID, record.RunID, record.NodeID, finalVariables)
	if err != nil {
		log.Printf("[TaskManager] Failed to complete Layer 1 task in Temporal: %v", err)
		return err
	}

	log.Printf("[TaskManager] Task %s marked as done in Layer 1!", record.NodeID)
	return nil
}

// StartLayer3Task persists a Layer 3 subtask and waits for frontend interaction.
func (tm *TaskManager) StartLayer3Task(payload engine.TaskPayload) error {
	taskID := payload.NodeID

	record := TaskRecord{
		WorkflowID:     payload.WorkflowID,
		RunID:          payload.RunID,
		NodeID:         payload.NodeID,
		TaskTemplateID: payload.TaskTemplateID,
		Status:         "PENDING_L3",
		Inputs:         payload.Inputs,
		CreatedAt:      time.Now(),
	}
	tm.db.SaveTask(taskID, record)
	log.Printf("[TaskManager] Persisted Layer 3 task to DB. TaskID: %s", taskID)
	
	return nil
}

// StartHTTPEndpoint exposes a static server and endpoints for task data and submission.
func (tm *TaskManager) StartHTTPEndpoint(addr string) {
	// Serve static files (HTML, CSS, JS)
	http.Handle("/", http.FileServer(http.Dir("./static")))

	// API to get all tasks
	http.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(tm.db.GetAllTasks())
	})

	// API to start a new Layer 1 workflow
	http.HandleFunc("/api/start", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}

		fileBytes, err := os.ReadFile("workflow.json")
		if err != nil {
			http.Error(w, "Failed to read workflow.json", http.StatusInternalServerError)
			return
		}

		var def engine.WorkflowDefinition
		if err := json.Unmarshal(fileBytes, &def); err != nil {
			http.Error(w, "Failed to parse workflow.json", http.StatusInternalServerError)
			return
		}

		workflowID := "nsw-demo-wf-" + time.Now().Format("150405")
		log.Printf("[TaskManager] Submitting Workflow ID: %s to Layer 1 via API", workflowID)

		err = tm.layer1Manager.StartWorkflow(context.Background(), workflowID, def, map[string]any{})
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to start workflow: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf(`{"status":"ok", "workflow_id":"%s"}`, workflowID)))
	})

	// API to submit a task
	http.HandleFunc("/api/submit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			TaskID string         `json:"task_id"`
			Output map[string]any `json:"output"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		record, exists := tm.db.GetTask(req.TaskID)
		if !exists {
			http.Error(w, "Task not found", http.StatusNotFound)
			return
		}

		if record.Status == "COMPLETED" {
			http.Error(w, "Task already completed", http.StatusBadRequest)
			return
		}

		// Merge original inputs with the new output from frontend
		finalOutput := make(map[string]any)
		for k, v := range record.Inputs {
			finalOutput[k] = v
		}
		for k, v := range req.Output {
			finalOutput[k] = v
		}

		// Update DB with the merged data and mark completed
		record.Inputs = finalOutput
		record.Status = "COMPLETED"
		tm.db.SaveTask(req.TaskID, record)

		// Complete the Layer 3 task in Temporal using layer2Manager
		err := tm.layer2Manager.TaskDone(context.Background(), record.WorkflowID, record.RunID, record.NodeID, finalOutput)
		if err != nil {
			log.Printf("[TaskManager] Temporal completion failed: %v", err)
			http.Error(w, "Temporal completion failed", http.StatusInternalServerError)
			return
		}

		log.Printf("[TaskManager] Task %s marked as done from Frontend!", req.TaskID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	log.Printf("[TaskManager] Starting HTTP API on %s...", addr)
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("[TaskManager] HTTP Server error: %v", err)
		}
	}()
}
