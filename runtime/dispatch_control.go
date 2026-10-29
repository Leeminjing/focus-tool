package runtime

import "fmt"

// Pause affects future assignment, not running jobs or durable execution facts.
// Recovery automatically starts paused if queued tasks were restored.
func (e *Engine) PauseDispatch() error {
	defer e.lock("pause_dispatch", Owner{}, "")()
	if e.closing {
		return ErrClosed
	}
	if e.fatal != nil {
		return e.fatal
	}
	e.paused = true
	return nil
}

// Registration is an inventory handshake, not an authorization grant. Every
// assigned task still passes the existing execution-time authority/lease hook.
func (e *Engine) ResumeDispatch(registered []TaskIdentity) error {
	defer e.lock("resume_dispatch", Owner{}, "")()
	if e.closing {
		return ErrClosed
	}
	if e.fatal != nil {
		return e.fatal
	}
	known := make(map[string]bool, len(registered))
	for _, identity := range registered {
		known[taskKey(identity.Owner, identity.TaskID)] = true
	}
	for key, item := range e.tasks {
		if item.State == "queued" && !known[key] {
			return fmt.Errorf("queued task has not been restored to trusted registration: %s", item.Task.ID)
		}
	}
	e.paused = false
	e.signal()
	return nil
}
