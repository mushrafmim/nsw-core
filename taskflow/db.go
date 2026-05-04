package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"
)

const dbFilePath = "/tmp/nsw_task_db.json"

// TaskRecord represents a task stored in the DB
type TaskRecord struct {
	WorkflowID       string         `json:"workflow_id"`
	RunID            string         `json:"run_id"`
	NodeID           string         `json:"node_id"`
	TaskTemplateID   string         `json:"task_template_id"`
	Layer2WorkflowID string         `json:"layer2_workflow_id"`
	Status           string         `json:"status"`
	Inputs           map[string]any `json:"inputs"`
	CreatedAt        time.Time      `json:"created_at"`
}

// TaskDB is an in-memory database for tasks
type TaskDB struct {
	mu    sync.RWMutex
	tasks map[string]TaskRecord
}

func NewTaskDB() *TaskDB {
	db := &TaskDB{
		tasks: make(map[string]TaskRecord),
	}

	// Try to load existing data
	data, err := os.ReadFile(dbFilePath)
	if err == nil {
		if err := json.Unmarshal(data, &db.tasks); err != nil {
			log.Printf("[TaskDB] Failed to parse existing DB file: %v", err)
		} else {
			log.Printf("[TaskDB] Loaded %d tasks from %s", len(db.tasks), dbFilePath)
		}
	} else if !os.IsNotExist(err) {
		log.Printf("[TaskDB] Failed to read DB file: %v", err)
	}

	return db
}

func (db *TaskDB) saveToFile() {
	data, err := json.MarshalIndent(db.tasks, "", "  ")
	if err != nil {
		log.Printf("[TaskDB] Failed to marshal tasks to JSON: %v", err)
		return
	}
	if err := os.WriteFile(dbFilePath, data, 0644); err != nil {
		log.Printf("[TaskDB] Failed to write DB file: %v", err)
	}
}

func (db *TaskDB) SaveTask(taskID string, record TaskRecord) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.tasks[taskID] = record
	db.saveToFile()
}

func (db *TaskDB) GetTask(taskID string) (TaskRecord, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	record, exists := db.tasks[taskID]
	return record, exists
}

func (db *TaskDB) GetTaskByLayer2WorkflowID(layer2WorkflowID string) (TaskRecord, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	for _, record := range db.tasks {
		if record.Layer2WorkflowID == layer2WorkflowID {
			return record, true
		}
	}
	return TaskRecord{}, false
}

func (db *TaskDB) GetAllTasks() []TaskRecord {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var list []TaskRecord
	for _, record := range db.tasks {
		list = append(list, record)
	}
	return list
}
