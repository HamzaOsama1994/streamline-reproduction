"""Hold ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED until killed (covers waits between grids in chain scripts)."""
import time
import power_guard as pg
pg.keep_awake()
while True:
    time.sleep(60)
