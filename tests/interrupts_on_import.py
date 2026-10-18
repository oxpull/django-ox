"""
A task module that raises KeyboardInterrupt while it is imported.

What Ctrl-C does to a process in the middle of an import. Nothing imports it
except ``ox_health --task-paths``, which must let the interrupt through.
"""

raise KeyboardInterrupt
