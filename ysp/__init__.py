from pathlib import Path

from .dataset import FANCDataset
from .database import ProofreadingDatabase
from .slackbot import app


fancpq_dir = Path(__file__).parent