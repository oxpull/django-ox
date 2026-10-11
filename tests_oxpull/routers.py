"""Routers used only by tests, activated via override_settings."""


class SplitTaskRouter:
    """Sends django_ox writes to another database; oxpull must refuse."""

    def db_for_write(self, model, **hints):
        if model._meta.app_label == "django_ox":
            return "tasksdb"
        return None


class ReplicaReadRouter:
    """Sends reads for both apps to the (empty) replica database."""

    def db_for_read(self, model, **hints):
        if model._meta.app_label in {"django_ox", "oxpull"}:
            return "replica"
        return None


class SplitRateWindowRouter:
    """Sends the rate window to another database; oxpull must refuse."""

    def db_for_write(self, model, **hints):
        if model._meta.label == "oxpull.OxpullRateWindow":
            return "tasksdb"
        return None
