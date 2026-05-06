package orchestrator

// TaskTemplateEntry defines how the TaskManager should initialise a task:
// which Task workflow definition to run and which JSONForms schemas to use for rendering.
type TaskTemplateEntry struct {
	TemplateID          string `json:"template_id"`
	TaskType            string `json:"task_type"`           // e.g. "APPLICATION"
	WorkflowID          string `json:"workflow_id"`         // Task workflow definition ID
	UserJsonFormsID     string `json:"user_jsonforms_id"`
	ReviewerJsonFormsID string `json:"reviewer_jsonforms_id"`
}

// TaskTemplateRegistry is a simple in-process registry mapping template IDs to their config.
type TaskTemplateRegistry struct {
	entries map[string]TaskTemplateEntry
}

// NewTaskTemplateRegistry returns an empty registry.
// Call Register to add templates, or use NewTaskTemplateRegistryFromDir to load from JSON files.
func NewTaskTemplateRegistry() *TaskTemplateRegistry {
	return &TaskTemplateRegistry{entries: make(map[string]TaskTemplateEntry)}
}

func (r *TaskTemplateRegistry) Register(entry TaskTemplateEntry) {
	r.entries[entry.TemplateID] = entry
}

func (r *TaskTemplateRegistry) Get(templateID string) (TaskTemplateEntry, bool) {
	entry, ok := r.entries[templateID]
	return entry, ok
}
