import uuid
import asyncio
from collections import defaultdict

import engine

"""
Missing API
===
DataFlock.document_put(environment, document_name, document)
DataFlock.document_get(environment, document_name)
DataFlock.document_delete(environment, document_name)
"""

class EnvironemntRunner:
    def set_dryrun(self):
        self.dryrun = True

    def __init__(self):
        self.cells = {}
        self._exposes = {}
        self._depends = defaultdict(set)
        self._running = set()
        self._dirty = set()
        self._live = {}
        self._dryrun = False
        self._callback = lambda *args: None
        self.kernel = engine.KernelProxy()

    def get_cells(self):
        return list(self.cells.keys())

    def cell_create(self, cell, live=True):

        # check duplicate exposure
        duplicate_names = set(cell.exposes).intersection(set(self._exposes.keys()))
        if duplicate_names:
            raise NameError("Tried to re-define previously exposed variables: %s" % (duplicate_names,))

        self.raise_if_loop(cell)

        # create cell
        cid = str(uuid.uuid4())
        self.cells[cid] = cell
        self.link_cell(cid, cell, live)
        
        self._callback("created:", cid, live, cell.code)
        
        if live:
            self.cell_run(cid)
        return cid

    def raise_if_loop(self, cell):
        # check definition loop
        start = set()
        for var in cell.exposes:
            start.update(self.depends(var))

        for c in start:
            for n in self.walk(c):
                if self.cells[n].exposes.intersection(cell.depends):
                    raise ValueError("Loop")

    def link_cell(self, cell_id, cell, live):
        for varname in cell.exposes:
            self._exposes[varname] = cell_id
        for varname in cell.depends:
            self._depends[varname].add(cell_id)
        self._live[cell_id] = live

    def unlink_cell(self, cell_id, cell):
        for varname in cell.exposes:
            del self._exposes[varname]
        for varname in cell.depends:
            self._depends[varname].remove(cell_id)
        del self._live[cell_id]

    def walk(self, cell_id):
        """Iterate over depending nodes in depth-first."""

        stack = {cell_id}
        
        while stack:
            current = stack.pop()
            children = self.dependent_cells(current)
            yield current
            stack.update(children)
        
    def dependent_cells(self, cid):
        """Return a set of all the cells that depend on variables defined in this cell."""
        deps = set()
        for v in self.cells[cid].exposes:
            deps.update(self.depends(v))
        return deps

    def cell_delete(self, cell_id):
        cell = self.cells[cell_id]
        del self.cells[cell_id]
        self.unlink_cell(cell_id, cell)
        
    def cell_get(self, cell_id):
        return self.cells[cell_id]

    def cell_update(self, cell_id, cell, live=True):
        self.raise_if_loop(cell)

        if cell_id in self.cells:
            self.unlink_cell(cell_id, self.cells[cell_id])
        self.cells[cell_id] = cell
        self.link_cell(cell_id, cell, live)
        self._callback("updated:", cell_id, live, cell.code)
        if live:
            self.cell_run(cell_id)

    async def __cell_run(self, cell_id): 
        cell = self.cells[cell_id]
        await self.kernel.run(cell.code, cell.depends, cell.exposes)
        self.on_cell_run_finished(cell_id)

    def _cell_run(self, cell_id):
        if not self._dryrun:
            loop = asyncio.get_event_loop()
            loop.create_task(self.__cell_run(cell_id))

    def cell_run(self, cell_id):
        self._callback("running", cell_id, self._live[cell_id])
        self._cell_run(cell_id)

        self._running.add(cell_id)
        for cid in self.walk(cell_id):
            self._callback("dirtied:", cid)
            self._dirty.add(cid)    

    def on_cell_run_finished(self, cell_id):
        self._running.remove(cell_id)
        self._dirty.remove(cell_id)
        self._callback("finished:", cell_id)

        # notify on new variables
        for varname in self.cells[cell_id].exposes:
            self._callback("updated", varname)
        # get cells that could run
        targets = self.dependent_cells(cell_id)
        
        # filter out cells with dirty parents
        for target in targets:
            for v in self.cells[target].depends:
                if self.is_dirty(self.exposes(v)):
                    break
            else:
                if self._live[target]:
                    self.cell_run(target)

    def is_dirty(self, cell_id):
        return cell_id in self._dirty

    def is_running(self, cell_id):
        return cell_id in self._running

    def exposes(self, varname):
        """Return the cell that exposes the variable."""
        return self._exposes[varname]

    def depends(self, varname):
        """Return a set of cells that depend on a variable."""
        return self._depends[varname]

    def set_callback(self, callback):
        self._callback = callback

    def get_variable(self, varname):
        return self.kernel.get(varname)


class DataFlock:
    def __init__(self):
        self.environments = {}

    def list_environments(self):
        return list(self.environments.keys())

    def environment_get(self, name):
        return self.environments[name]

    def environment_create(self, name):
        if name in self.environments:
            raise KeyError("Environment already exists")

        er = EnvironemntRunner()
        self.environments[name] = er
        return er

    def environemnt_delete(self, name):
        del self.environments[name]

