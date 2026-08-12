"""`python -m catwalk` == the catwalk CLI (`catwalk start` spawns the server
through this entry point so the child needs no console script on PATH)."""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
