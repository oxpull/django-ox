"""
A task module that calls sys.exit() while it is imported.

What a module with a command-line entry point and no ``__main__`` guard does
when something imports it. Nothing imports it except a worker rebuilding a
row whose task_path names it, or ``ox_health --task-paths``.
"""

import sys

sys.exit(0)
