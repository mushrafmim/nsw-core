package orchestrator

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	engine "github.com/OpenNSW/go-temporal-workflow"
	"github.com/OpenNSW/nsw-task-flow/store"
)

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

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

// safeMockTaskStore is a thread-safe task store for use in all tests.
// Using a mutex means tests run with -race won't flag the map accesses.
type safeMockTaskStore struct {
	mu    sync.RWMutex
	tasks map[string]store.TaskRecord
}

func newSafeMockTaskStore() *safeMockTaskStore {
	return &safeMockTaskStore{tasks: make(map[string]store.TaskRecord)}
}

func (s *safeMockTaskStore) SaveTask(record store.TaskRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[record.TaskID] = record
}

func (s *safeMockTaskStore) GetTask(taskID string) (store.TaskRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.tasks[taskID]
	return r, ok
}

func (s *safeMockTaskStore) GetTaskByLayer2WorkflowID(layer2WorkflowID string) (store.TaskRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, r := range s.tasks {
		if r.Layer2WorkflowID == layer2WorkflowID {
			return r, true
		}
	}
	return store.TaskRecord{}, false
}

func (s *safeMockTaskStore) GetAllTasks() []store.TaskRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.TaskRecord
	for _, r := range s.tasks {
		out = append(out, r)
	}
	return out
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newTestRegistry() *TaskTemplateRegistry {
	r := NewTaskTemplateRegistry()
	r.Register(TaskTemplateEntry{
		TemplateID:      "test_template",
		TaskType:        "TEST",
		WorkflowID:      "test_workflow_v1",
		UserJsonFormsID: "user_form",
	})
	return r
}

func writeTempTaskJSON(t *testing.T, content string) (path string, cleanup func()) {
	t.Helper()
	f, err := os.CreateTemp("", "task_*.json")
	if err != nil {
		t.Fatalf("could not create temp file: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("could not write temp file: %v", err)
	}
	f.Close()
	return f.Name(), func() { os.Remove(f.Name()) }
}

func noopCallback(_, _, _ string, _ map[string]any) error { return nil }

// ---------------------------------------------------------------------------
// Lifecycle integration test (original)
// ---------------------------------------------------------------------------

func TestTaskManager_Lifecycle(t *testing.T) {
	storeMock := newSafeMockTaskStore()
	registry := newTestRegistry()

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

	callbackCalled := false
	onCompleted := func(l1WorkflowID string, l1RunID string, l1NodeID string, finalVars map[string]any) error {
		callbackCalled = true
		return nil
	}

	path, cleanup := writeTempTaskJSON(t, `{"id": "test_workflow_v1", "nodes": []}`)
	defer cleanup()

	tm := NewTaskManager(storeMock, registry, mockL2, onCompleted).WithTaskDefPath(path)

	// 1. StartTask
	payload := engine.TaskPayload{
		WorkflowID:     "l1-workflow",
		RunID:          "l1-run",
		NodeID:         "node-1",
		TaskTemplateID: "test_template",
		Inputs:         map[string]any{"userform.name": "Alice"},
	}

	if err := tm.StartTask(payload); err != nil {
		t.Fatalf("StartTask failed: %v", err)
	}
	if !layer2Called {
		t.Error("expected Layer 2 sub-workflow to be started")
	}

	tasks := storeMock.GetAllTasks()
	if len(tasks) != 1 {
		t.Fatalf("expected exactly 1 task record, got %d", len(tasks))
	}
	task := tasks[0]
	if task.Status != "STARTING" {
		t.Errorf("expected status 'STARTING', got '%s'", task.Status)
	}
	if task.Layer1WorkflowID != "l1-workflow" {
		t.Errorf("expected L1 workflow 'l1-workflow', got '%s'", task.Layer1WorkflowID)
	}

	// 2. HandleTask — generic_user_input
	payloadL2 := engine.TaskPayload{
		WorkflowID:     task.Layer2WorkflowID,
		RunID:          "l2-run",
		NodeID:         "l2-node",
		TaskTemplateID: "generic_user_input",
	}
	if err := tm.HandleTask(payloadL2); err != nil {
		t.Fatalf("HandleTask failed: %v", err)
	}

	task, _ = storeMock.GetTask(task.TaskID)
	if task.Status != "PENDING_USER" {
		t.Errorf("expected status 'PENDING_USER', got '%s'", task.Status)
	}
	if task.Layer2RunID != "l2-run" {
		t.Errorf("expected Layer 2 run ID 'l2-run', got '%s'", task.Layer2RunID)
	}

	// 3. CompleteTaskStep
	l2TaskDoneCalled := false
	mockL2.taskDoneFunc = func(ctx context.Context, workflowID string, runID string, activityID string, result map[string]any) error {
		l2TaskDoneCalled = true
		if workflowID != task.Layer2WorkflowID {
			t.Errorf("expected L2 workflow ID %s, got %s", task.Layer2WorkflowID, workflowID)
		}
		if activityID != "l2-node" {
			t.Errorf("expected active activity ID 'l2-node', got %s", activityID)
		}
		return nil
	}

	userData := map[string]any{
		"userform": map[string]any{
			"applicant_name": "Alice",
			"email":          "alice@example.com",
		},
	}
	if err := tm.CompleteTaskStep(context.Background(), task.TaskID, userData); err != nil {
		t.Fatalf("CompleteTaskStep failed: %v", err)
	}
	if !l2TaskDoneCalled {
		t.Error("expected Layer 2 TaskDone to be called")
	}

	task, _ = storeMock.GetTask(task.TaskID)
	userform, ok := task.Data["userform"].(map[string]any)
	if !ok {
		t.Fatal("expected 'userform' to be a nested map")
	}
	if userform["email"] != "alice@example.com" {
		t.Errorf("expected userform.email 'alice@example.com', got '%v'", userform["email"])
	}

	// 4. HandleLayer2Completion
	finalVars := map[string]any{"reviewerform.review_outcome": "approve"}
	if err := tm.HandleLayer2Completion(task.Layer2WorkflowID, finalVars); err != nil {
		t.Fatalf("HandleLayer2Completion failed: %v", err)
	}

	task, _ = storeMock.GetTask(task.TaskID)
	if task.Status != "COMPLETED" {
		t.Errorf("expected task status 'COMPLETED', got '%s'", task.Status)
	}
	if !callbackCalled {
		t.Error("expected parent wake-up callback to be invoked")
	}
}

// ---------------------------------------------------------------------------
// StartTask — error paths
// ---------------------------------------------------------------------------

func TestStartTask_UnknownTemplateID(t *testing.T) {
	tm := NewTaskManager(newSafeMockTaskStore(), newTestRegistry(), &mockTemporalManager{}, noopCallback)

	err := tm.StartTask(engine.TaskPayload{
		WorkflowID:     "l1",
		TaskTemplateID: "does_not_exist",
	})
	if err == nil {
		t.Fatal("expected error for unknown template ID, got nil")
	}
}

func TestStartTask_MissingTaskDefFile(t *testing.T) {
	tm := NewTaskManager(newSafeMockTaskStore(), newTestRegistry(), &mockTemporalManager{}, noopCallback).
		WithTaskDefPath("/tmp/this_file_does_not_exist_xyz.json")

	err := tm.StartTask(engine.TaskPayload{
		WorkflowID:     "l1",
		TaskTemplateID: "test_template",
	})
	if err == nil {
		t.Fatal("expected error for missing task def file, got nil")
	}
}

func TestStartTask_MalformedTaskDefFile(t *testing.T) {
	path, cleanup := writeTempTaskJSON(t, `{not valid json`)
	defer cleanup()

	tm := NewTaskManager(newSafeMockTaskStore(), newTestRegistry(), &mockTemporalManager{}, noopCallback).
		WithTaskDefPath(path)

	err := tm.StartTask(engine.TaskPayload{
		WorkflowID:     "l1",
		TaskTemplateID: "test_template",
	})
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
}

func TestStartTask_Layer2ManagerError(t *testing.T) {
	mockL2 := &mockTemporalManager{
		startWorkflowFunc: func(_ context.Context, _ string, _ engine.WorkflowDefinition, _ map[string]any) error {
			return errors.New("temporal unavailable")
		},
	}

	path, cleanup := writeTempTaskJSON(t, `{"id":"wf","nodes":[]}`)
	defer cleanup()

	tm := NewTaskManager(newSafeMockTaskStore(), newTestRegistry(), mockL2, noopCallback).
		WithTaskDefPath(path)

	err := tm.StartTask(engine.TaskPayload{
		WorkflowID:     "l1",
		TaskTemplateID: "test_template",
	})
	if err == nil {
		t.Fatal("expected error when layer2 StartWorkflow fails, got nil")
	}
}

// ---------------------------------------------------------------------------
// HandleTask — error paths
// ---------------------------------------------------------------------------

func TestHandleTask_UnknownLayer2WorkflowID(t *testing.T) {
	tm := NewTaskManager(newSafeMockTaskStore(), newTestRegistry(), &mockTemporalManager{}, noopCallback)

	err := tm.HandleTask(engine.TaskPayload{
		WorkflowID:     "workflow-that-was-never-registered",
		TaskTemplateID: "generic_user_input",
	})
	if err == nil {
		t.Fatal("expected error for unknown layer2 workflow ID, got nil")
	}
}

func TestHandleTask_UnknownTaskTemplateID(t *testing.T) {
	db := newSafeMockTaskStore()
	db.SaveTask(store.TaskRecord{
		TaskID:           "task-1",
		Layer2WorkflowID: "l2-workflow-1",
		Status:           "STARTING",
		Data:             map[string]any{},
	})

	tm := NewTaskManager(db, newTestRegistry(), &mockTemporalManager{}, noopCallback)

	err := tm.HandleTask(engine.TaskPayload{
		WorkflowID:     "l2-workflow-1",
		TaskTemplateID: "not_a_real_template",
	})
	if err == nil {
		t.Fatal("expected error for unknown task_template_id in HandleTask, got nil")
	}
}

func TestHandleTask_ExternalReviewPath(t *testing.T) {
	db := newSafeMockTaskStore()
	db.SaveTask(store.TaskRecord{
		TaskID:           "task-ext",
		Layer2WorkflowID: "l2-ext-workflow",
		Status:           "STARTING",
		Data:             map[string]any{},
	})

	tm := NewTaskManager(db, newTestRegistry(), &mockTemporalManager{}, noopCallback)

	err := tm.HandleTask(engine.TaskPayload{
		WorkflowID:     "l2-ext-workflow",
		RunID:          "run-1",
		NodeID:         "node-ext",
		TaskTemplateID: "generic_external_review",
	})
	if err != nil {
		t.Fatalf("HandleTask for generic_external_review failed: %v", err)
	}

	task, _ := db.GetTask("task-ext")
	if task.Status != "QUEUED_EXTERNALLY" {
		t.Errorf("expected status QUEUED_EXTERNALLY, got %s", task.Status)
	}
}

// ---------------------------------------------------------------------------
// CompleteTaskStep — error paths
// ---------------------------------------------------------------------------

func TestCompleteTaskStep_UnknownTaskID(t *testing.T) {
	tm := NewTaskManager(newSafeMockTaskStore(), newTestRegistry(), &mockTemporalManager{}, noopCallback)

	err := tm.CompleteTaskStep(context.Background(), "task-ghost", map[string]any{"x": 1})
	if err == nil {
		t.Fatal("expected error for unknown task ID, got nil")
	}
}

func TestCompleteTaskStep_AlreadyCompleted(t *testing.T) {
	db := newSafeMockTaskStore()
	db.SaveTask(store.TaskRecord{
		TaskID: "task-done",
		Status: "COMPLETED",
		Data:   map[string]any{},
	})

	tm := NewTaskManager(db, newTestRegistry(), &mockTemporalManager{}, noopCallback)

	err := tm.CompleteTaskStep(context.Background(), "task-done", map[string]any{"x": 1})
	if err == nil {
		t.Fatal("expected error for already-completed task, got nil")
	}
}

// ---------------------------------------------------------------------------
// HandleLayer2Completion — edge cases
// ---------------------------------------------------------------------------

func TestHandleLayer2Completion_UnknownWorkflowID_ReturnsNil(t *testing.T) {
	tm := NewTaskManager(newSafeMockTaskStore(), newTestRegistry(), &mockTemporalManager{}, noopCallback)

	err := tm.HandleLayer2Completion("unknown-workflow", map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("expected nil for unknown workflow, got: %v", err)
	}
}

func TestHandleLayer2Completion_CallbackError_TaskStillMarkedCompleted(t *testing.T) {
	db := newSafeMockTaskStore()
	db.SaveTask(store.TaskRecord{
		TaskID:           "task-cb-err",
		Layer2WorkflowID: "l2-cb-err",
		Status:           "PENDING_USER",
		Data:             map[string]any{},
	})

	callbackErr := errors.New("layer1 unreachable")
	tm := NewTaskManager(db, newTestRegistry(), &mockTemporalManager{},
		func(_, _, _ string, _ map[string]any) error { return callbackErr },
	)

	err := tm.HandleLayer2Completion("l2-cb-err", map[string]any{})
	if !errors.Is(err, callbackErr) {
		t.Fatalf("expected callback error to be propagated, got: %v", err)
	}

	task, _ := db.GetTask("task-cb-err")
	if task.Status != "COMPLETED" {
		t.Errorf("expected status COMPLETED even after callback error, got %s", task.Status)
	}
}

// ---------------------------------------------------------------------------
// setNestedKey — unit tests
// ---------------------------------------------------------------------------

func TestSetNestedKey_SingleLevel(t *testing.T) {
	m := map[string]any{}
	setNestedKey(m, "name", "Alice")
	if m["name"] != "Alice" {
		t.Errorf("expected 'Alice', got %v", m["name"])
	}
}

func TestSetNestedKey_TwoLevels(t *testing.T) {
	m := map[string]any{}
	setNestedKey(m, "user.email", "alice@example.com")

	sub, ok := m["user"].(map[string]any)
	if !ok {
		t.Fatal("expected 'user' to be a map")
	}
	if sub["email"] != "alice@example.com" {
		t.Errorf("expected 'alice@example.com', got %v", sub["email"])
	}
}

func TestSetNestedKey_ThreeLevels(t *testing.T) {
	m := map[string]any{}
	setNestedKey(m, "a.b.c", 42)

	a, ok := m["a"].(map[string]any)
	if !ok {
		t.Fatal("expected 'a' to be a map")
	}
	b, ok := a["b"].(map[string]any)
	if !ok {
		t.Fatal("expected 'a.b' to be a map")
	}
	if b["c"] != 42 {
		t.Errorf("expected 42, got %v", b["c"])
	}
}

func TestSetNestedKey_EmptyPath_IsNoop(t *testing.T) {
	m := map[string]any{"existing": "value"}
	setNestedKey(m, "", "should_not_appear")
	if len(m) != 1 {
		t.Errorf("expected map to be unchanged, got %v", m)
	}
}

func TestSetNestedKey_OverwritesNonMapIntermediate(t *testing.T) {
	m := map[string]any{"user": "not-a-map"}
	setNestedKey(m, "user.email", "alice@example.com")

	sub, ok := m["user"].(map[string]any)
	if !ok {
		t.Fatal("expected 'user' to be replaced with a map")
	}
	if sub["email"] != "alice@example.com" {
		t.Errorf("expected 'alice@example.com', got %v", sub["email"])
	}
}

func TestSetNestedKey_MergesIntoExistingMap(t *testing.T) {
	m := map[string]any{
		"user": map[string]any{"name": "Bob"},
	}
	setNestedKey(m, "user.email", "bob@example.com")

	sub := m["user"].(map[string]any)
	if sub["name"] != "Bob" {
		t.Error("existing key 'name' should not be removed")
	}
	if sub["email"] != "bob@example.com" {
		t.Errorf("expected 'bob@example.com', got %v", sub["email"])
	}
}
