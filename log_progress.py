import logging
import os
from time import gmtime, strftime

current_time = strftime("%Y_%m_%dT%H_%M_%S", gmtime())
lgdr = f"./logging/{current_time}"
os.makedirs(lgdr, exist_ok=True)

log_file = f"{lgdr}/results.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)
