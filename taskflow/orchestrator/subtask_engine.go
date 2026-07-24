// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Lanka Software Foundation

package orchestrator

import (
	"context"
	"errors"

	engine "github.com/OpenNSW/core/workflow"
	"github.com/mushrafmim/fsm"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
)

// SubTaskEngine runs Task micro-journeys as fsm executions. *fsm.Engine satisfies
// it directly. The TaskManager depends only on these two verbs — starting an
// execution and resuming its currently-parked task — so the concrete engine (and
// the Temporal client it holds) is constructed and wired in from the application
// layer, not here.
type SubTaskEngine interface {
	// Start launches a Task micro-journey: one fsm execution of chart under id,
	// seeded with input. The returned WorkflowRun is unused by the TaskManager
	// (the parent parks and is later woken via the completion hook).
	Start(ctx context.Context, id string, chart fsm.Chart, input fsm.Data) (client.WorkflowRun, error)

	// CompleteByExecution resumes an execution's currently-parked subtask, keyed
	// only on the execution id. The engine reads the parked task's id and finishes
	// it with result, which advances the walk.
	CompleteByExecution(ctx context.Context, executionID string, result fsm.Result) error
}

// SubTaskHandler adapts one fsm task run to the TaskManager's StartSubTask logic.
// Wire it into the engine with fsm.WithHandler so every chart state resolves its
// TaskTemplateID through the plugin registry, extensions, and output namespace
// that StartSubTask already owns — the chart itself stays plugin-agnostic.
//
// A suspended subtask (StartSubTask returns activity.ErrResultPending) is mapped
// to fsm.ErrParked so the engine leaves the task open for a later
// CompleteByExecution. A synchronous completion returns its data under the
// default (empty-command) transition, so a non-branching chart advances with no
// command to invent.
func (tm *TaskManager) SubTaskHandler(ctx context.Context, req fsm.TaskRequest) (fsm.Result, error) {
	payload := engine.TaskPayload{
		WorkflowID:     req.ExecutionID,
		RunID:          req.TaskID,
		NodeID:         string(req.State),
		TaskTemplateID: req.TaskTemplateID,
		Inputs:         map[string]any(req.Data),
	}

	out, err := tm.StartSubTask(ctx, payload)
	if errors.Is(err, activity.ErrResultPending) {
		return fsm.Result{}, fsm.ErrParked
	}
	if err != nil {
		return fsm.Result{}, err
	}
	return fsm.Result{Data: fsm.Data(out)}, nil
}

// CompletionHandler adapts an execution's End-state completion to
// HandleTaskCompletion. Wire it with fsm.WithCompletionHandler. The executionID
// is the TaskWorkflowID StartTask launched under, which HandleTaskCompletion
// resolves back to the task record before waking the parent workflow.
func (tm *TaskManager) CompletionHandler(ctx context.Context, executionID string, final fsm.Data) error {
	return tm.HandleTaskCompletion(ctx, executionID, final)
}
