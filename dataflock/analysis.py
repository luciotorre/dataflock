import ast

class Cell:
    def __init__(self, code):
        self.code = code
        self.depends = set()
        self.exposes = set()
        
        tree = ast.parse(code)
        for n in ast.walk(tree):
            if isinstance(n, ast.Name):
                if not n.id in self.exposes:
                    self.depends.add(n.id)
            if isinstance(n, ast.Assign):
                for name in n.targets:
                    self.exposes.add(name.id)
            
    def __eq__(self, other):
        return other.code == self.code
