"""
pytest configuration and shared fixtures.
"""
import pytest
import pandas as pd
import sys
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).parents[1]))
