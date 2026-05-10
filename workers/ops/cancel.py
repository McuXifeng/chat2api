async def run(worker, task_id: str):
    """Find which slot owns the in-flight task and abort its in-page fetch."""
    task = await worker.driver.get_task(task_id)
    if not task:
        return
    instance_id = task.get("instance_id")
    if not instance_id:
        return
    slot = worker.slots.get(instance_id)
    if slot is None:
        return
    await slot.abort(task_id)
