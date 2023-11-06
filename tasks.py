import asyncio
from invoke import task

from extractor.nfe import run


@task(default=True)
def nfe(c):
    asyncio.run(run())
