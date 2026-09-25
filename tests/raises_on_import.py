"""
A task module that raises TypeError while it is imported, not ImportError.

What a module declaring ``@task(max_attempts=5)`` does on Django 6.0, whose
``task()`` takes no extra keyword arguments. Nothing imports it except a
worker rebuilding a row whose task_path names it.
"""

raise TypeError("task() got an unexpected keyword argument 'max_attempts'")
