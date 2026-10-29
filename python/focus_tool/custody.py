"""Best-effort cancellation requests; never evidence that a process exited."""
from .client import RemoteRejected


async def cancel_jobs(run, job_ids):
    outcomes = []
    for job in dict.fromkeys(job_ids):
        outcome = {"owner": dict(run.owner), "job_id": job}
        try:
            status = await run.status(job)
        except RemoteRejected as error:
            outcome.update(state="not_submitted" if str(error) == "unknown task" else "failed", error=str(error))
        except Exception as error:
            outcome.update(state="failed", error=str(error))
        else:
            if status["state"] == "unknown":
                outcome.update(state="unknown", error="execution remains quarantined")
            elif status["state"] in {"completed", "failed", "cancelled", "skipped"}:
                outcome.update(state="already_terminal", execution_state=status["state"])
            else:
                try:
                    await run.cancel(job)
                    outcome["state"] = "cancel_requested"
                except Exception as error:
                    outcome.update(state="failed", error=str(error))
        outcomes.append(outcome)
    return outcomes
