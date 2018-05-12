import ast
import builtins
from enum import Enum

import attr


BUILTINS = set(dir(builtins))


class Cell:
    def __init__(self, code):
        self.code = code
        self.depends = find_missing_vars(code)
        self.exposes = set()

        tree = ast.parse(code)
        for n in ast.walk(tree):
            if isinstance(n, ast.Assign):
                for name in n.targets:
                    self.exposes.add(name.id)

    def __eq__(self, other):
        return other.code == self.code


def find_missing_vars(code):
    """
    Find missing variables.
    """
    ast_root = ast.parse(code)
    scope_root = ScopeTreeNode.build_from_ast_node(ast_root)

    missing_vars = set()

    pending = [scope_root, ]

    # traverse the scope tree, trying to find cases of undefined variables
    while pending:
        scope = pending.pop()

        # check the variable operations in order, to detect uses before definitions
        known_vars = set()
        for var_use in scope.variable_uses:
            if var_use.kind == VariableUsage.Kind.SET:
                known_vars.add(var_use.name)
            elif var_use.name not in known_vars and var_use.name not in BUILTINS:
                if not scope.variable_in_parent_scopes(var_use.name):
                    missing_vars.add(var_use.name)
            elif var_use.kind == VariableUsage.Kind.DEL:
                known_vars.remove(var_use.name)

        pending.extend(scope.children)

    return missing_vars


@attr.s
class VariableUsage:
    """
    A use of a variable in the code.
    """
    class Kind(Enum):
        """
        Kind of variable usage (read it, set it, etc).
        """
        READ = 1
        SET = 2
        DEL = 3
        UNKNOWN = 4

    name = attr.ib()
    kind = attr.ib()


@attr.s
class ScopeTreeNode:
    """
    A node in the scopes tree, representing a level of variables scope.
    """
    class Kind(Enum):
        """
        Kind of scope (global scope behaves different from local scopes, etc).
        """
        GLOBAL = 0
        LOCAL = 1

    kind = attr.ib()
    ast_node = attr.ib()
    parent = attr.ib(default=None)
    children = attr.ib(default=attr.Factory(list))
    sets_vars = attr.ib(default=attr.Factory(list))
    variable_uses = attr.ib(default=attr.Factory(list))

    def print_as_tree(self, indentation=0):
        """
        Print the tree in a nice readable way, with indentation for each scope level.
        """
        uses = ('{kind}:{name}'.format(kind=use.kind.name, name=use.name)
                for use in self.variable_uses)
        print(' ' * indentation, self.ast_node.__class__.__name__,
              self.kind.name, ', '.join(uses))

        for child in self.children:
            child.print_as_tree(indentation=indentation + 2)

    @classmethod
    def build_from_ast_node(cls, root_ast_node):
        """
        Build a tree of scopes with their variables.
        Non-recursive implementation, it's more complex but avoids max recursion errors.
        """
        assert isinstance(root_ast_node, ast.Module)

        root_scope_node = ScopeTreeNode(
            kind=ScopeTreeNode.Kind.GLOBAL,
            ast_node=root_ast_node,
        )

        # each pending element is composed of 3 parts:
        # nearest parent scope, ast node, optional or not
        # (optional code is code in a block that can be not ran, so the deletes should be ignored)
        pending = [(root_scope_node, root_ast_node, False)]

        while pending:
            scope_node, ast_node, is_optional = pending.pop(0)
            new_children = []

            for child_ast_node in ast.iter_child_nodes(ast_node):
                child_scope = scope_node

                if isinstance(child_ast_node, ast.Name):
                    child_scope = scope_node.visit_child_name(child_ast_node, is_optional)
                elif isinstance(child_ast_node, ast.arg):
                    child_scope = scope_node.visit_child_arg(child_ast_node)
                elif isinstance(child_ast_node, ast.FunctionDef):
                    child_scope = scope_node.visit_child_function(child_ast_node)
                elif isinstance(child_ast_node, ast.ClassDef):
                    child_scope = scope_node.visit_child_class(child_ast_node)
                elif isinstance(child_ast_node, (ast.If, ast.While, ast.For, ast.Try)):
                    is_optional = True

                # ...but if I'm the "finally" from a try-except, I'm no longer optional
                if isinstance(ast_node, ast.Try) and child_ast_node in ast_node.finalbody:
                    is_optional = False

                new_children.append((child_scope, child_ast_node, is_optional))

            pending = new_children + pending

        return root_scope_node

    def visit_child_name(self, ast_node, is_optional):
        """
        Variable being used in child ast node, infor it to us (current scope).
        """
        var_name = ast_node.id
        if isinstance(ast_node.ctx, ast.Load):
            use_kind = VariableUsage.Kind.READ
        elif isinstance(ast_node.ctx, ast.Del) and not is_optional:
            use_kind = VariableUsage.Kind.DEL
        elif isinstance(ast_node.ctx, ast.Store):
            use_kind = VariableUsage.Kind.SET
        else:
            use_kind = VariableUsage.Kind.UNKNOWN

        # log the variable usage being done in our scope
        self.variable_uses.append(VariableUsage(
            name=var_name,
            kind=use_kind,
        ))

        return self

    def visit_child_arg(self, ast_node):
        """
        Argument being defined in child ast node, treat it as a variable being set.
        """
        self.variable_uses.append(VariableUsage(
            name=ast_node.arg,
            kind=VariableUsage.Kind.SET,
        ))

        return self

    def visit_child_function(self, ast_node):
        """
        Function definition in child ast node, treat is as a variable being set, but also create
        a new scope for the child node.
        """
        # log the new variable being created in our scope
        self.variable_uses.append(VariableUsage(
            name=ast_node.name,
            kind=VariableUsage.Kind.SET,
        ))

        # the child is creating its own scope for its children
        child_scope = ScopeTreeNode(
            kind=ScopeTreeNode.Kind.LOCAL,
            ast_node=ast_node,
            parent=self,
        )
        self.children.append(child_scope)

        return child_scope

    def visit_child_class(self, ast_node):
        """
        Class definition in child ast node, treat is as a variable being set, but also create
        a new scope for the child node.
        """
        # log the new variable being created in our scope
        self.variable_uses.append(VariableUsage(
            name=ast_node.name,
            kind=VariableUsage.Kind.SET,
        ))

        # the child is creating its own scope for its children
        child_scope = ScopeTreeNode(
            kind=ScopeTreeNode.Kind.LOCAL,
            ast_node=ast_node,
            parent=self,
        )
        self.children.append(child_scope)

        return child_scope

    def variable_in_parent_scopes(self, variable_name):
        """
        Find out if a variable exists in the parent scopes of this scope.
        """
        scope = self.parent

        while scope is not None:
            variables_set = set(use.name for use in scope.variable_uses
                                if use.kind == VariableUsage.Kind.SET)
            if variable_name in variables_set:
                return True
            else:
                scope = scope.parent

        return False
