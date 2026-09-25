"""ArgsForms that stored schedules in the tests can name."""

from django import forms

from django_ox.registry import ArgsForm


class FilterArgs(ArgsForm):
    """
    A label and a JSON field, as an admin form offering "advanced options"
    as a textarea would declare them.

    The JSON field is the point. ArgsForm stores it as the string a person
    typed, and the form parses it with json.loads, which accepts the bare
    token Infinity. The row validates and saves, and every database then
    refuses the task it enqueues.
    """

    label = forms.CharField()
    filters = forms.JSONField(required=False)
