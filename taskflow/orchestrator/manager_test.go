package orchestrator

import (
	"context"
	"errors"
	"os"
	"testing"

	engine "github.com/OpenNSW/go-temporal-workflow"
	"github.com/OpenNSW/nsw-task-flow/store"
)

// mockTemporalManager implements engine.TemporalManager for unit testing
type mockTemporalManager struct {
	startWorkflowFunc func(ctx context.Context, workflowID string, def engine.WorkflowDefinition, initialVars map[string]any) error
	taskDoneFunc      func(ctx context.Context, workflowID string, runID string, activityID string, result map[string]any) error
	startWorkerFunc   func() error
	stopWorkerFunc    func()
}

func (m *mockTemporalManager) StartWorkflow(ctx context.Context, workflowID string, def engine.WorkflowDefinition, initialVars map[string]any) error {
	if m.startWorkflowFunc != nil {
		return m.startWorkflowFunc(ctx, workflowID, def, initialVars)
	}
	return nil
}

func (m *mockTemporalManager) TaskDone(ctx context.Context, workflowID string, runID string, activityID string, result map[string]any) error {
	if m.taskDoneFunc != nil {
		return m.taskDoneFunc(ctx, workflowID, runID, activityID, result)
	}
	return nil
}

func (m *mockTemporalManager) StartWorker() error {
	if m.startWorkerFunc != nil {
		return m.startWorkerFunc()
	}
	return nil
}

func (m *mockTemporalManager) StopWorker() {
	if m.stopWorkerFunc != nil {
		m.stopWorkerFunc()
	}
}

func (m *mockTemporalManager) TaskUpdate(ctx context.Context, workflowID, runID string, update engine.UpdateEvent) error {
	return nil
}

func (m *mockTemporalManager) GetStatus(ctx context.Context, workflowID string) (*engine.WorkflowInstance, error) {
	return nil, nil
}

// mockTaskStore implements store.TaskStore for unit testing
type mockTaskStore struct {
	tasks map[string]store.TaskRecord
}

func newMockTaskStore() *mockTaskStore {
	return &mockTaskStore{tasks: make(map[string]store.TaskRecord)}
}

func (m *mockTaskStore) SaveTask(record store.TaskRecord) {
	m.tasks[record.TaskID] = record
}

func (m *mockTaskStore) GetTask(taskID string) (store.TaskRecord, bool) {
	record, ok := m.tasks[taskID]
	return record, ok
}

func (m *mockTaskStore) GetTaskByLayer2WorkflowID(layer2WorkflowID string) (store.TaskRecord, bool) {
	for _, record := range m.tasks {
		if record.Layer2WorkflowID == layer2WorkflowID {
			return record, true
		}
	}
	return store.TaskRecord{}, false
}

func (m *mockTaskStore) GetAllTasks() []store.TaskRecord {
	var list []store.TaskRecord
	for _, record := range m.tasks {
		list = append(list, record)
	}
	return list
}

func TestTaskManager_Lifecycle(t *testing.T) {
	storeMock := newMockTaskStore()
	registry := NewTaskTemplateRegistry()

	// Register a dummy template
	registry.Register(TaskTemplateEntry{
		TemplateID:      "test_template",
		TaskType:        "TEST",
		WorkflowID:      "test_workflow_v1",
		UserJsonFormsID: "user_form",
	})

	// Setup mock Layer 2 Temporal manager
	layer2Called := false
	mockL2 := &mockTemporalManager{
		startWorkflowFunc: func(ctx context.Context, workflowID string, def engine.WorkflowDefinition, initialVars map[string]any) error {
			layer2Called = true
			if initialVars["_task_id"] == "" {
				return errors.New("missing task ID in initialVars")
			}
			return nil
		},
	}

	// Setup TaskManager with inline callback to simulate Layer 1 wake-up
	callbackCalled := false
	onCompleted := func(l1WorkflowID string, l1RunID string, l1NodeID string, finalVars map[string]any) error {
		callbackCalled = true
		return nil
	}

	// Create a temporary task definition JSON to avoid needing a real task.json
	tm := NewTaskManager(storeMock, registry, mockL2, onCompleted).WithTaskDefPath("test_task.json")

	// Write test_task.json
	testTaskJSON := []byte(`{"id": "test_workflow_v1", "nodes": []}`)
	err := os.WriteFile("test_task.json", testTaskJSON, 0644)
	if err != nil {
		t.Fatalf("Failed to write test task definition: %v", err)
	}
	defer os.Remove("test_task.json")

	// 1. Test StartTask
	payload := engine.TaskPayload{
		WorkflowID:     "l1-workflow",
		RunID:          "l1-run",
		NodeID:         "node-1",
		TaskTemplateID: "test_template",
		Inputs:         map[string]any{"userform.name": "Alice"},
	}

	err = tm.StartTask(payload)
	if err != nil {
		t.Fatalf("StartTask failed: %v", err)
	}

	if !layer2Called {
		t.Error("Expected Layer 2 sub-workflow to be started")
	}

	// Verify database record creation
	tasks := storeMock.GetAllTasks()
	if len(tasks) != 1 {
		t.Fatalf("Expected exactly 1 task record, got %d", len(tasks))
	}

	task := tasks[0]
	if task.Status != "STARTING" {
		t.Errorf("Expected status 'STARTING', got '%s'", task.Status)
	}
	if task.Layer1WorkflowID != "l1-workflow" {
		t.Errorf("Expected L1 workflow 'l1-workflow', got '%s'", task.Layer1WorkflowID)
	}

	// 2. Test HandleTask (simulating Layer 2 activation of generic_user_input)
	payloadL2 := engine.TaskPayload{
		WorkflowID:     task.Layer2WorkflowID,
		RunID:          "l2-run",
		NodeID:         "l2-node",
		TaskTemplateID: "generic_user_input",
	}

	err = tm.HandleTask(payloadL2)
	if err != nil {
		t.Fatalf("HandleTask failed: %v", err)
	}

	// Refresh task record
	task, _ = storeMock.GetTask(task.TaskID)
	if task.Status != "PENDING_USER" {
		t.Errorf("Expected status 'PENDING_USER', got '%s'", task.Status)
	}
	if task.Layer2RunID != "l2-run" {
		t.Errorf("Expected Layer 2 run ID 'l2-run', got '%s'", task.Layer2RunID)
	}

	// 3. Test CompleteTaskStep (simulating user submitting data)
	l2TaskDoneCalled := false
	mockL2.taskDoneFunc = func(ctx context.Context, workflowID string, runID string, activityID string, result map[string]any) error {
		l2TaskDoneCalled = true
		if workflowID != task.Layer2WorkflowID {
			t.Errorf("Expected L2 workflow ID %s, got %s", task.Layer2WorkflowID, workflowID)
		}
		if activityID != "l2-node" {
			t.Errorf("Expected active activity ID 'l2-node', got %s", activityID)
		}
		return nil
	}

	userData := map[string]any{
		"userform": map[string]any{
			"applicant_name": "Alice",
			"email":          "alice@example.com",
		},
	}
	err = tm.CompleteTaskStep(context.Background(), task.TaskID, userData)
	if err != nil {
		t.Fatalf("CompleteTaskStep failed: %v", err)
	}

	if !l2TaskDoneCalled {
		t.Error("Expected Layer 2 TaskDone to be called")
	}

	// Verify that userform data was merged correctly
	task, _ = storeMock.GetTask(task.TaskID)
	userform, ok := task.Data["userform"].(map[string]any)
	if !ok {
		t.Fatal("Expected 'userform' to be a nested map")
	}
	if userform["email"] != "alice@example.com" {
		t.Errorf("Expected userform.email 'alice@example.com', got '%v'", userform["email"])
	}

	// 4. Test HandleLayer2Completion (simulating Layer 2 sub-workflow completion)
	finalVars := map[string]any{"reviewerform.review_outcome": "approve"}
	err = tm.HandleLayer2Completion(task.Layer2WorkflowID, finalVars)
	if err != nil {
		t.Fatalf("HandleLayer2Completion failed: %v", err)
	}

	task, _ = storeMock.GetTask(task.TaskID)
	if task.Status != "COMPLETED" {
		t.Errorf("Expected task status 'COMPLETED', got '%s'", task.Status)
	}

	if !callbackCalled {
		t.Error("Expected parent wake-up callback to be invoked")
	}
}
