import os

import pandas as pd

from common.py import files, logs
from mtga import scryfall


if __name__ == "__main__":

    # Log
    log = logs.get_logger()

    # Pull
    data = scryfall.get_latest_all_cards_data()

    # Write
    cur_ts = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
    path = os.path.expanduser(f"~/dat/scryfall/all_cards_{cur_ts}.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    files.write(data, path)
    log.info(f"Wrote {len(data)} bytes to {path}.")
