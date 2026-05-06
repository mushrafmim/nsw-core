package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	engine "github.com/OpenNSW/go-temporal-workflow"
	"github.com/OpenNSW/nsw-task-flow/internal/orchestrator"
)

type Server struct {
	manager       *orchestrator.TaskManager
	layer1Manager engine.TemporalManager
}

func NewServer(manager *orchestrator.TaskManager, layer1Manager engine.TemporalManager) *Server {
	return &Server{manager: manager, layer1Manager: layer1Manager}
}

func (s *Server) Start(addr string) {
	// Serve static files
	http.Handle("/", http.FileServer(http.Dir("./static")))

	// API endpoints
	http.HandleFunc("/api/tasks", s.handleGetTasks)
	http.HandleFunc("/api/start", s.handleStartWorkflow)
	// Unified task interaction endpoint: POST /api/task/{taskID}
	http.HandleFunc("/api/task/", s.handleTaskInteraction)

	log.Printf("[API] Starting HTTP server on %s...", addr)
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("[API] HTTP server error: %v", err)
		}
	}()
}

func (s *Server) handleGetTasks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.manager.GetDB().GetAllTasks())
}

func (s *Server) handleStartWorkflow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	// Accept an optional applicant_name to pre-fill the userform
	var req struct {
		ApplicantName string `json:"applicant_name"`
	}
	// Ignore decode errors — applicant_name is optional
	json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck

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

	workflowID := "nsw-phyto-" + time.Now().Format("150405")
	log.Printf("[API] Starting Layer 1 workflow %s (applicant=%s)", workflowID, req.ApplicantName)

	initialVars := map[string]any{}
	if req.ApplicantName == "" {
		req.ApplicantName = "John Doe"
	}
	initialVars["applicant_name"] = req.ApplicantName

	err = s.layer1Manager.StartWorkflow(context.Background(), workflowID, def, initialVars)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to start workflow: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "workflow_id": workflowID})
}

// handleTaskInteraction is the unified endpoint: POST /api/task/{taskID}
// It routes the payload to the correct Layer 2 activity using the stored ActiveActivityID.
func (s *Server) handleTaskInteraction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	// Extract taskID from path: /api/task/{taskID}
	taskID := strings.TrimPrefix(r.URL.Path, "/api/task/")
	if taskID == "" {
		http.Error(w, "missing task ID in path", http.StatusBadRequest)
		return
	}

	db := s.manager.GetDB()
	record, exists := db.GetTask(taskID)
	if !exists {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	if record.Status == "COMPLETED" {
		http.Error(w, "task already completed", http.StatusConflict)
		return
	}

	// The payload from the UI is a namespaced map matching the JSONForms structure,
	// e.g. {"userform": {...}} or {"reviewerform": {...}}
	var payload map[string]any
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	// Merge submitted data into the stored namespaced Data map
	if record.Data == nil {
		record.Data = make(map[string]any)
	}
	for k, v := range payload {
		record.Data[k] = v
	}
	db.SaveTask(record)

	// Wake up the exact Layer 2 activity using the stored coordinates
	log.Printf("[API] Waking Layer 2 activity %s in workflow %s (task %s)",
		record.ActiveActivityID, record.Layer2WorkflowID, taskID)

	err := s.manager.GetLayer2Manager().TaskDone(
		context.Background(),
		record.Layer2WorkflowID,
		record.Layer2RunID,
		record.ActiveActivityID,
		record.Data, // pass full namespaced state back to the workflow
	)
	if err != nil {
		log.Printf("[API] Failed to wake Layer 2 activity: %v", err)
		http.Error(w, "failed to resume workflow", http.StatusInternalServerError)
		return
	}

	log.Printf("[API] Task %s resumed successfully", taskID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
