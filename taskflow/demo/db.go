package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	"github.com/OpenNSW/nsw-task-flow/store"
)

const dbFilePath = "/tmp/nsw_task_db.json"

// TaskDB is an in-memory, file-backed database for task records used specifically for the demo.
type TaskDB struct {
	mu    sync.RWMutex
	tasks map[string]store.TaskRecord // keyed by TaskID
}

func NewTaskDB() *TaskDB {
	db := &TaskDB{
		tasks: make(map[string]store.TaskRecord),
	}

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
		log.Printf("[TaskDB] Failed to marshal tasks: %v", err)
		return
	}
	if err := os.WriteFile(dbFilePath, data, 0644); err != nil {
		log.Printf("[TaskDB] Failed to write DB file: %v", err)
	}
}

func (db *TaskDB) SaveTask(record store.TaskRecord) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.tasks[record.TaskID] = record
	db.saveToFile()
}

func (db *TaskDB) GetTask(taskID string) (store.TaskRecord, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	record, exists := db.tasks[taskID]
	return record, exists
}

func (db *TaskDB) GetTaskByWorkflowID(workflowID string) (store.TaskRecord, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	for _, record := range db.tasks {
		if record.TaskWorkflowID == workflowID {
			return record, true
		}
	}
	return store.TaskRecord{}, false
}

func (db *TaskDB) GetAllTasks() []store.TaskRecord {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var list []store.TaskRecord
	for _, record := range db.tasks {
		list = append(list, record)
	}
	return list
}
