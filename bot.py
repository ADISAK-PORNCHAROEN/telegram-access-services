"""Run the installed application with state rooted beside this entrypoint."""

import asyncio
from pathlib import Path

from app.bootstrap import main


if __name__ == "__main__":
    asyncio.run(main(Path(__file__).resolve().parent))
