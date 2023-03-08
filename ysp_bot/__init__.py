from pathlib import Path

from .dataset import FANCDataset
from .database import ProofreadingDatabase
from .slackbot import app


ysp_dir = Path(__file__).parent