# One distribution, one version: oxpull ships inside django-ox, so its version
# is django-ox's. tools/check_release.py refuses a literal here.
from django_ox import __version__

__all__ = ["__version__"]
