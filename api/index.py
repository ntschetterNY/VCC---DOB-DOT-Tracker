import sys
import os

# Add the vorea_violations directory to path so Flask can find app.py
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'vorea_violations'))

from app import app  # noqa: F401  (Vercel looks for 'app' in this file)
