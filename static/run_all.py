#!/usr/bin/env python3
"""
Orchestrator to run all static dataset pipelines present in this folder.
"""
import os
import sys
import glob
import importlib
from datetime import datetime

# Ensure project root (one level up) is in sys.path for imports
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

def main():
    print(f"[INFO] Starting all static pipelines at {datetime.now()}")
    static_dir = os.path.dirname(os.path.abspath(__file__))
    # Find all python files in this directory
    pattern = os.path.join(static_dir, '*.py')
    files = glob.glob(pattern)
    # Exclude helper and orchestrator scripts
    exclude = {'static_pipeline.py', 'run_all.py', '__init__.py'}
    for filepath in sorted(files):
        filename = os.path.basename(filepath)
        if filename in exclude:
            continue
        module_name = os.path.splitext(filename)[0]
        full_module = f'static.{module_name}'
        try:
            module = importlib.import_module(full_module)
            if hasattr(module, 'run_pipeline'):
                print(f"[INFO] Running pipeline: {module_name}")
                module.run_pipeline()
            else:
                print(f"[WARN] {module_name} has no run_pipeline(); skipping.")
        except Exception as e:
            print(f"[ERROR] Exception while running {module_name}: {e}")

if __name__ == '__main__':
    main()