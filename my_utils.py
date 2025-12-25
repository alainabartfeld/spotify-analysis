#%%
import logging
import json
import os
import pandas as pd
from pathlib import Path

#%%
def setup_logging(log_file_path):
    logging.basicConfig(
        filename=log_file_path,
        filemode="a",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )