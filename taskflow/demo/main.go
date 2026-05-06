// Package main is the entry point for the NSW Task Flow demo.
//
// Run from the repo root:
//
//	go run ./demo
//
// It wires together a two-layer Temporal orchestrator and serves the split-pane
// portal UI on http://localhost:8080.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	engine "github.com/OpenNSW/go-temporal-workflow"
	"github.com/OpenNSW/nsw-task-flow/orchestrator"
	"go.temporal.io/sdk/client"
)

func main() {
	// 1. Temporal client
	c, err := client.Dial(client.Options{
		HostPort: client.DefaultHostPort,
	})
	if err != nil {
		log.Fatalln("Unable to create Temporal client", err)
	}
	defer c.Close()

	// 2. Store & Task Template Registry
	// Templates are loaded from ./demo/templates/*.json — add a new file to register a new flow.
	db := NewTaskDB()
	registry := orchestrator.NewTaskTemplateRegistry()
	if err := loadTemplates(registry, "demo/templates"); err != nil {
		log.Fatalln("Failed to load task template registry:", err)
	}

	// 3. Set up Temporal Managers (layer1 and layer2) with deferred task manager wiring
	var tm *orchestrator.TaskManager

	// --- Layer 1 handlers ---
	layer1TaskHandler := func(payload engine.TaskPayload) error {
		log.Printf("\n[Layer 1] Task activated: node=%s template=%s\n", payload.NodeID, payload.TaskTemplateID)
		if tm != nil {
			return tm.StartTask(payload)
		}
		return nil
	}

	layer1CompletionHandler := func(workflowID string, finalVariables map[string]any) error {
		log.Printf("\n[Layer 1] Workflow %s completed. Final state: %v\n", workflowID, finalVariables)
		return nil
	}

	layer1Manager := engine.NewTemporalManager(
		c,
		"nsw-layer1-queue",
		layer1TaskHandler,
		layer1CompletionHandler,
	)

	// --- Layer 2 handlers ---
	layer2TaskHandler := func(payload engine.TaskPayload) error {
		log.Printf("\n[Layer 2] Task activated: node=%s template=%s\n", payload.NodeID, payload.TaskTemplateID)
		if tm != nil {
			return tm.HandleTask(payload)
		}
		return nil
	}

	layer2CompletionHandler := func(workflowID string, finalVariables map[string]any) error {
		log.Printf("\n[Layer 2] Workflow %s completed. Final state: %v\n", workflowID, finalVariables)
		if tm != nil {
			return tm.HandleLayer2Completion(workflowID, finalVariables)
		}
		return nil
	}

	layer2Manager := engine.NewTemporalManager(
		c,
		"nsw-layer2-queue",
		layer2TaskHandler,
		layer2CompletionHandler,
	)

	// 4. Wire everything together
	onTaskCompleted := func(layer1WorkflowID string, layer1RunID string, layer1NodeID string, finalVariables map[string]any) error {
		err := layer1Manager.TaskDone(context.Background(), layer1WorkflowID, layer1RunID, layer1NodeID, finalVariables)
		if err != nil {
			log.Printf("[TaskManager] Failed to wake Layer 1 workflow %s: %v", layer1WorkflowID, err)
			return err
		}
		log.Printf("[TaskManager] Woke Layer 1 workflow %s node %s", layer1WorkflowID, layer1NodeID)
		return nil
	}

	tm = orchestrator.NewTaskManager(db, registry, layer2Manager, onTaskCompleted).
		WithTaskDefPath("demo/task.json")

	apiServer := newServer(tm, layer1Manager)
	apiServer.start(":8080")

	// 5. Start workers
	log.Println("Starting Layer 1 Temporal Worker...")
	if err := layer1Manager.StartWorker(); err != nil {
		log.Fatalln("Unable to start layer 1 worker:", err)
	}
	defer layer1Manager.StopWorker()

	log.Println("Starting Layer 2 Temporal Worker...")
	if err := layer2Manager.StartWorker(); err != nil {
		log.Fatalln("Unable to start layer 2 worker:", err)
	}
	defer layer2Manager.StopWorker()

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("Shutting down gracefully...")
}
