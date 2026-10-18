"""Kept so older commands keep working: runs the whole GEORA pipeline (all regions).
Use `python -m pipeline.run` (optionally `--only us-nyc,fr`)."""
import sys
from pipeline.run import main

if __name__ == "__main__":
    sys.argv = [a for a in sys.argv if a != "--site"]
    main()
