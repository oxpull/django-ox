"""
Importing this module raises ImproperlyConfigured on Django before 6.0.

Every module that imports django.tasks imports this one first. A failed import
is not cached, so every attempt raises again.
"""

from ._django_gate import require_django_6

require_django_6()
