#!/bin/bash
cd backend
source ../venv/bin/activate
export PYTHONWARNINGS="ignore::DeprecationWarning"
python3 app_simple.py