# NSW Task Flow Demo

This repository demonstrates a 3-layer architecture for handling tasks and workflows using a Temporal-backed graph interpreter. 

Currently, we have implemented **Layer 1: Top-Level Workflow**. It reads a BPMN graph (represented as JSON) that models a simple application form process containing a Start node, a Task node (Application Form), and an End node.

## Prerequisites

- [Go](https://golang.org/doc/install) 1.20+
- [Temporal CLI](https://docs.temporal.io/cli/) (for running a local Temporal server)

## 1. Start Local Temporal Server

You need a running Temporal cluster for the workflow engine to orchestrate the tasks. You can quickly spin up a development cluster using the Temporal CLI.

Open a new terminal window and run:

```bash
temporal server start-dev
```

This will start a local Temporal server and a Web UI. You can view the Web UI at [http://localhost:8233](http://localhost:8233).

## 2. Run the Demo

Once Temporal is running, execute the demo application in another terminal:

```bash
go run main.go
```

### What happens when you run it:
1. The program starts a Temporal Worker listening to the `nsw-task-queue`.
2. It loads the graph interpreter workflow definition from `workflow.json`.
3. It kicks off the top-level workflow on the Temporal cluster.
4. The workflow reaches the "application_task" node and fires the local `TaskActivationHandler`, simulating an external system being notified.
5. A background goroutine simulates external work (e.g. a user filling out an application form) and calls `TaskDone()` 3 seconds later.
6. The workflow resumes, finishes the End node, and triggers the `WorkflowCompletionHandler`.

You should see output similar to this:
```
2026/05/04 15:00:00 Starting Temporal Worker...
2026/05/04 15:00:00 Submitting Workflow ID: nsw-demo-wf-150000
2026/05/04 15:00:00 Workflow started. Waiting for completion...

[Task Handler] Task Activated! Template: application_form_task, NodeID: application_task:12345-uuid
[External System] Working on task for 3 seconds...
[External System] Task successfully marked as done in Temporal!

[Completion Handler] Workflow nsw-demo-wf-150000 Completed! Final state: map[status:application_approved]
```
