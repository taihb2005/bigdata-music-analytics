"""Streamlit entry point for the published Gold dashboard."""
from pathlib import Path
from runpy import run_path

run_path(str(Path(__file__).resolve().parents[1] / "batch" / "gold" / "dashboard.py"), run_name="__main__")
