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

/*
Package orchestrator provides a domain-driven TaskManager designed to decouple high-level
macro journeys from low-level interactive processes.

The system uses a hierarchical, decoupled design:

1. Workflow (Macro Journey):
   The high-level orchestrating workflow (parent workflow). When the macro journey hits a
   "Task" node, it executes a callback that calls TaskManager.StartTask().

2. Task (Micro Journey):
   A self-contained micro-flow executing child tasks (such as document upload, fee payment,
   or physical inspections). The Task runs as an independent workflow process under the hood
   (defined by a JSON workflow definition).

3. SubTask (Interaction Steps):
   Individual, potentially asynchronous execution nodes inside the Task (e.g., waiting for
   a user form submission, or queuing a request in an external agency portal). These are
   dispatched via StartSubTask() and resumed via CompleteTaskStep().

Flow Diagram:
              [Parent Workflow]
                     │
                     ▼ (StartTask)
              [TaskManager] ────► [Task Record created in DB]
                     │
                     ▼ (StartTaskWorkflow)
              [Task Workflow]
                     │
                     ▼ (StartSubTask)
              [SubTask Node] (e.g., PENDING_USER status)
                     │
                     ▼ (CompleteTaskStep)
           [Resume SubTask & Continue]
                     │
                     ▼ (TaskWorkflow completed)
           [HandleTaskCompletion]
                     │
                     ▼ (Callback)
              [Resume Parent Workflow]
*/

// TaskManager orchestrates decoupled tasks and interactions under parent workflows.
// It bridges macro-level workflows and micro-level interactive tasks via a single DB entry per task.
type TaskManager struct {
	db                  store.TaskStore
	registry            *TaskTemplateRegistry
	onTaskCompleted     func(parentWorkflowID string, parentRunID string, parentNodeID string, finalVariables map[string]any) error
	taskWorkflowManager engine.TemporalManager
	taskDefPath         string
}

// NewTaskManager creates a TaskManager instance.
//
//   - db                  — the persistence/in-memory task store.
//   - registry            — registry holding definitions of task capabilities.
//   - taskWorkflowManager — the TemporalManager used to start and complete Task sub-workflows.
//   - onTaskCompleted     — callback invoked when a Task workflow finishes;
//     typically invokes Parent.TaskDone to resume the parent workflow using stored coordinates.
func NewTaskManager(
	db store.TaskStore,
	registry *TaskTemplateRegistry,
	taskWorkflowManager engine.TemporalManager,
	onTaskCompleted func(parentWorkflowID string, parentRunID string, parentNodeID string, finalVariables map[string]any) error,
) *TaskManager {
	return &TaskManager{
		db:                  db,
		registry:            registry,
		onTaskCompleted:     onTaskCompleted,
		taskWorkflowManager: taskWorkflowManager,
		taskDefPath:         "task.json",
	}
}

// WithTaskDefPath overrides the path to the Task workflow definition JSON.
// Useful when running tests or running from an alternate directory.
func (tm *TaskManager) WithTaskDefPath(path string) *TaskManager {
	tm.taskDefPath = path
	return tm
}

// StartTask is called by the parent workflow engine when it activates a TASK node.
// It looks up the template registry, creates a single DB record with parent
// coordinates, and kicks off the Task's internal workflow.
func (tm *TaskManager) StartTask(payload engine.TaskPayload) error {
	regEntry, ok := tm.registry.Get(payload.TaskTemplateID)
	if !ok {
		return fmt.Errorf("unknown task_template_id: %s", payload.TaskTemplateID)
	}

	taskID := "task-" + uuid.New().String()[:8]
	taskWorkflowID := "task-wf-" + taskID

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
		ParentWorkflowID: payload.WorkflowID,
		ParentRunID:      payload.RunID,
		ParentNodeID:     payload.NodeID,
		TaskWorkflowID:   taskWorkflowID,
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

	err = tm.taskWorkflowManager.StartWorkflow(context.Background(), taskWorkflowID, def, initialData)
	if err != nil {
		return fmt.Errorf("failed to start task workflow: %v", err)
	}
	log.Printf("[TaskManager] Started task workflow %s for task %s", taskWorkflowID, taskID)
	return nil
}

// StartSubTask is called by the Task's workflow engine when it activates an interaction step.
// It routes to the correct capability handler based on task_template_id.
func (tm *TaskManager) StartSubTask(payload engine.TaskPayload) error {
	record, exists := tm.db.GetTaskByWorkflowID(payload.WorkflowID)
	if !exists {
		return fmt.Errorf("[StartSubTask] no task record found for workflow %s", payload.WorkflowID)
	}

	record.TaskRunID = payload.RunID
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
		// In a real implementation, dispatch external API requests here.
		log.Printf("[TaskManager] Task %s dispatched to external reviewer at node %s", record.TaskID, payload.NodeID)

	default:
		return fmt.Errorf("unknown task_template_id inside Task workflow: %s", payload.TaskTemplateID)
	}

	tm.db.SaveTask(record)
	return nil
}

// HandleTaskCompletion is called when a Task workflow hits its END node.
// It marks the task complete and fires the onTaskCompleted callback to resume the parent workflow.
func (tm *TaskManager) HandleTaskCompletion(workflowID string, finalVariables map[string]any) error {
	record, exists := tm.db.GetTaskByWorkflowID(workflowID)
	if !exists {
		// Not a workflow we own — safe to ignore.
		return nil
	}

	log.Printf("[TaskManager] Task workflow %s completed for task %s", workflowID, record.TaskID)

	record.Status = "COMPLETED"
	tm.db.SaveTask(record)

	err := tm.onTaskCompleted(record.ParentWorkflowID, record.ParentRunID, record.ParentNodeID, finalVariables)
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
// data and resume the active step in the corresponding Task workflow.
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

	log.Printf("[TaskManager] Waking active activity %s in workflow %s (task %s)",
		record.ActiveActivityID, record.TaskWorkflowID, taskID)

	err := tm.taskWorkflowManager.TaskDone(
		ctx,
		record.TaskWorkflowID,
		record.TaskRunID,
		record.ActiveActivityID,
		record.Data, // pass full namespaced state back to the workflow
	)
	if err != nil {
		return fmt.Errorf("failed to resume task workflow: %w", err)
	}

	return nil
}

// GetDB returns the underlying task store.
func (tm *TaskManager) GetDB() store.TaskStore {
	return tm.db
}

// GetTaskWorkflowManager returns the Task's TemporalManager.
func (tm *TaskManager) GetTaskWorkflowManager() engine.TemporalManager {
	return tm.taskWorkflowManager
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
