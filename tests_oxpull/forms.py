"""ArgsForms that stored schedules in the tests can name."""

from django import forms

from django_ox.registry import ArgsForm


class ValueArgs(ArgsForm):
    """
    The one argument tests_oxpull.tasks.plain_echo takes, as a JSON field.

    ArgsForm stores a JSON field as the string a person typed, and the form
    parses it with json.loads, which accepts the bare token Infinity. A row
    holding that validates and saves, and every supported database then
    refuses the task row it enqueues.
    """

    value = forms.JSONField()
