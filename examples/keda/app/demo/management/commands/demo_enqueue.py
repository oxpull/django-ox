from typing import Any

from django.core.management.base import BaseCommand, CommandParser

from demo.tasks import build_report


class Command(BaseCommand):
    help = "Enqueue COUNT build_report tasks on the reports queue."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--count", type=int, required=True)
        parser.add_argument("--seconds", type=float, required=True)

    def handle(self, *args: Any, **options: Any) -> None:
        for _ in range(options["count"]):
            build_report.enqueue(options["seconds"])
        self.stdout.write(f"enqueued {options['count']} x build_report")
