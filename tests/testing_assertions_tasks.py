"""Module-level tasks for tests/test_testing_assertions.py."""

from django_ox.compat import task


@task
def note(label):
    pass


@task
def greet(name, *, loud=False):
    pass


@task(queue_name="isolated-recipe")
def isolated_task(label):
    return label
