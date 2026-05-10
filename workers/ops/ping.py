async def run(driver, slot, task_id: str, payload: dict):
    ok = await slot.ping()
    await driver.append_done(
        task_id,
        status="done" if ok else "failed",
        output={"alive": bool(ok)},
    )
