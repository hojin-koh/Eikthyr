import luigi as lg

def run(tasks):
    return lg.build(tasks, local_scheduler=True, log_level='WARNING', detailed_summary=True, workers=1)
