package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	engine "github.com/OpenNSW/go-temporal-workflow"
	"github.com/OpenNSW/nsw-task-flow/store"
	"github.com/google/uuid"
)

// TaskManager orchestrates the two-layer architecture described in the design doc.
// It bridges Layer 1 (macro journey) and Layer 2 (micro flow) via a single DB entry per task.
type TaskManager struct {
	db       store.TaskStore
	registry *TaskTemplateRegistry
	// onTaskCompleted is called when a Layer 2 sub-workflow finishes, to resume Layer 1.
	onTaskCompleted func(layer1WorkflowID string, layer1RunID string, layer1NodeID string, finalVariables map[string]any) error
	// layer2Manager starts and wakes Layer 2 sub-workflows.
	layer2Manager engine.TemporalManager
	// taskDefPath is the path to the Layer 2 workflow definition JSON (default: "task.json").
	taskDefPath string
}

// NewTaskManager creates a TaskManager.
//
//   - layer2       — the TemporalManager for the Layer 2 queue.
//   - onTaskCompleted — callback invoked when a Layer 2 workflow completes;
//     typically calls layer1Manager.TaskDone with the stored parent coordinates.
func NewTaskManager(
	db store.TaskStore,
	registry *TaskTemplateRegistry,
	layer2 engine.TemporalManager,
	onTaskCompleted func(layer1WorkflowID string, layer1RunID string, layer1NodeID string, finalVariables map[string]any) error,
) *TaskManager {
	return &TaskManager{
		db:              db,
		registry:        registry,
		onTaskCompleted: onTaskCompleted,
		layer2Manager:   layer2,
		taskDefPath:     "task.json",
	}
}

// WithTaskDefPath overrides the path to the Layer 2 workflow definition JSON.
// Useful when running from a directory that isn't the repo root.
func (tm *TaskManager) WithTaskDefPath(path string) *TaskManager {
	tm.taskDefPath = path
	return tm
}

// StartTask is called by the Layer 1 engine when it activates a TASK node.
// It looks up the template registry, creates the single DB record with Layer 1 parent
// coordinates, and kicks off the Layer 2 workflow.
func (tm *TaskManager) StartTask(payload engine.TaskPayload) error {
	regEntry, ok := tm.registry.Get(payload.TaskTemplateID)
	if !ok {
		return fmt.Errorf("unknown task_template_id: %s", payload.TaskTemplateID)
	}

	taskID := "task-" + uuid.New().String()[:8]
	layer2WorkflowID := "layer2-" + taskID

	initialData := make(map[string]any)
	for k, v := range payload.Inputs {
		setNestedKey(initialData, k, v)
	}
	initialData["_task_id"] = taskID

	record := store.TaskRecord{
		TaskID:           taskID,
		TaskType:         regEntry.TaskType,
		UserFormID:       regEntry.UserJsonFormsID,
		ReviewerFormID:   regEntry.ReviewerJsonFormsID,
		Status:           "STARTING",
		Layer1WorkflowID: payload.WorkflowID,
		Layer1RunID:      payload.RunID,
		Layer1NodeID:     payload.NodeID,
		Layer2WorkflowID: layer2WorkflowID,
		Data:             initialData,
		CreatedAt:        time.Now(),
	}
	tm.db.SaveTask(record)
	log.Printf("[TaskManager] Created Task record %s (template=%s)", taskID, payload.TaskTemplateID)

	fileBytes, err := os.ReadFile(tm.taskDefPath)
	if err != nil {
		return fmt.Errorf("failed to read %s: %v", tm.taskDefPath, err)
	}
	var def engine.WorkflowDefinition
	if err := json.Unmarshal(fileBytes, &def); err != nil {
		return fmt.Errorf("failed to parse %s: %v", tm.taskDefPath, err)
	}

	err = tm.layer2Manager.StartWorkflow(context.Background(), layer2WorkflowID, def, initialData)
	if err != nil {
		return fmt.Errorf("failed to start Layer 2 workflow: %v", err)
	}
	log.Printf("[TaskManager] Started Layer 2 workflow %s for task %s", layer2WorkflowID, taskID)
	return nil
}

// StartSubTask is called by the Layer 2 engine when it activates a TASK node inside the sub-workflow.
// It routes to the correct capability handler based on task_template_id.
func (tm *TaskManager) StartSubTask(payload engine.TaskPayload) error {
	record, exists := tm.db.GetTaskByLayer2WorkflowID(payload.WorkflowID)
	if !exists {
		return fmt.Errorf("[StartSubTask] no task record found for Layer 2 workflow %s", payload.WorkflowID)
	}

	record.Layer2RunID = payload.RunID
	record.ActiveActivityID = payload.NodeID

	for k, v := range payload.Inputs {
		setNestedKey(record.Data, k, v)
	}

	switch payload.TaskTemplateID {
	case "generic_user_input":
		record.Status = "PENDING_USER"
		log.Printf("[TaskManager] Task %s waiting for user input at node %s", record.TaskID, payload.NodeID)

	case "generic_external_review":
		record.Status = "QUEUED_EXTERNALLY"
		// In a real implementation, call the external API here before returning.
		log.Printf("[TaskManager] Task %s dispatched to external reviewer at node %s", record.TaskID, payload.NodeID)

	default:
		return fmt.Errorf("unknown Layer 2 task_template_id: %s", payload.TaskTemplateID)
	}

	tm.db.SaveTask(record)
	return nil
}

// HandleTaskCompletion is called when a Layer 2 workflow (representing a Task) hits its END node.
// It marks the task complete and fires the onTaskCompleted callback to resume Layer 1.
func (tm *TaskManager) HandleTaskCompletion(workflowID string, finalVariables map[string]any) error {
	record, exists := tm.db.GetTaskByLayer2WorkflowID(workflowID)
	if !exists {
		// Not a Layer 2 workflow we own — safe to ignore.
		return nil
	}

	log.Printf("[TaskManager] Layer 2 workflow %s completed for task %s", workflowID, record.TaskID)

	record.Status = "COMPLETED"
	tm.db.SaveTask(record)

	err := tm.onTaskCompleted(record.Layer1WorkflowID, record.Layer1RunID, record.Layer1NodeID, finalVariables)
	if err != nil {
		log.Printf("[TaskManager] Failed to execute task completion callback for %s: %v", record.TaskID, err)
		return err
	}

	log.Printf("[TaskManager] Successfully processed completion for task %s", record.TaskID)
	return nil
}

// GetTask retrieves a single task record by its ID.
func (tm *TaskManager) GetTask(taskID string) (store.TaskRecord, bool) {
	return tm.db.GetTask(taskID)
}

// GetAllTasks retrieves all task records in the store.
func (tm *TaskManager) GetAllTasks() []store.TaskRecord {
	return tm.db.GetAllTasks()
}

// CompleteTaskStep is the public API for external clients or portals to submit form/interaction
// data and resume the active step in the corresponding Layer 2 sub-workflow.
func (tm *TaskManager) CompleteTaskStep(ctx context.Context, taskID string, payload map[string]any) error {
	record, exists := tm.db.GetTask(taskID)
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	if record.Status == "COMPLETED" {
		return fmt.Errorf("task %s already completed", taskID)
	}

	// Merge submitted data into the stored namespaced Data map
	if record.Data == nil {
		record.Data = make(map[string]any)
	}
	for k, v := range payload {
		record.Data[k] = v
	}
	tm.db.SaveTask(record)

	log.Printf("[TaskManager] Waking Layer 2 activity %s in workflow %s (task %s)",
		record.ActiveActivityID, record.Layer2WorkflowID, taskID)

	err := tm.layer2Manager.TaskDone(
		ctx,
		record.Layer2WorkflowID,
		record.Layer2RunID,
		record.ActiveActivityID,
		record.Data, // pass full namespaced state back to the workflow
	)
	if err != nil {
		return fmt.Errorf("failed to resume Layer 2 workflow: %w", err)
	}

	return nil
}

// GetDB returns the underlying task store.
func (tm *TaskManager) GetDB() store.TaskStore {
	return tm.db
}

// GetLayer2Manager returns the Layer 2 TemporalManager.
func (tm *TaskManager) GetLayer2Manager() engine.TemporalManager {
	return tm.layer2Manager
}

// setNestedKey sets a value in a map using a dot-separated path.
func setNestedKey(m map[string]any, dotPath string, value any) {
	if dotPath == "" {
		return
	}
	for i := 0; i < len(dotPath); i++ {
		if dotPath[i] == '.' {
			key := dotPath[:i]
			rest := dotPath[i+1:]
			sub, ok := m[key]
			if !ok || sub == nil {
				sub = make(map[string]any)
			}
			subMap, ok := sub.(map[string]any)
			if !ok {
				subMap = make(map[string]any)
			}
			setNestedKey(subMap, rest, value)
			m[key] = subMap
			return
		}
	}
	m[dotPath] = value
}
