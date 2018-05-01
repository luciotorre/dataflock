import multiprocessing
import json
import asyncio


class KernelProxy:
    def __init__(self):
        self.variables = {}

    def interrupt(self):
        pass

    def restart(self):
        self.kill()
        self.restart()

    def start(self):
        self.variables = {}

    def kill(self):
        pass

    async def run(self, code, depends, exposes):
        local_vars = dict((k, self.variables[k]) for k in depends)
        await asyncio.sleep(0)
        print("execing", code, local_vars)
        exec(code, globals(), local_vars)
        print("execd", code, local_vars)
        self.variables.update(dict((k, local_vars[k]) for k in exposes))
        print("final_state", self.variables)

    def get(self, varname):
        return self.variables[varname]
