# Makes the shared `lakehouse` package (notebooks/lakehouse) importable from the category folder, the tests and run.py.
import os
import sys

_NOTEBOOKS = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _NOTEBOOKS not in sys.path:
    sys.path.insert(0, _NOTEBOOKS)
