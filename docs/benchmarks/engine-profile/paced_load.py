"""Shared offered-load check for engine_profile calibration and paired decisions."""


def backlog_allowance(mode, operation, row):
    # A cut is taken at the scheduled arrival of the last included request.
    # Each contributing stream can still have that one call outstanding.
    # Older records predate the explicit stream count. The memory workload
    # has 16 Batch issuers at both placements; each remaining class has one.
    streams = row.get("streams", 16 if mode == "memory" and operation == "Batch" else 1)
    if not 1 <= streams <= row["offered"]:
        raise ValueError(f"invalid stream count for {operation}: {streams}")
    return max(2, streams, row["offered"] // 100)


def load_kept(record):
    if not record["settings"].get("RATE", 0):
        return True
    for operation, row in record["classes"].items():
        allowance = backlog_allowance(record["mode"], operation, row)
        if row["outstanding_end"] > allowance or row["outstanding_end"] - row["outstanding_half"] > allowance:
            return False
    return True
