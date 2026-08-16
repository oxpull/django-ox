from datetime import UTC, datetime, timedelta

import pytest

from django_ox.cron import CronExpression


class TestParsing:
    def test_star_fields_are_unrestricted(self):
        cron = CronExpression("* * * * *")
        assert cron.minutes == tuple(range(60))
        assert cron.hours == tuple(range(24))
        assert cron.days_of_month == tuple(range(1, 32))
        assert cron.months == tuple(range(1, 13))
        assert cron.days_of_week == tuple(range(7))

    def test_lists_ranges_and_steps(self):
        cron = CronExpression("*/15 8-18/2 1,15 * 1-5")
        assert cron.minutes == (0, 15, 30, 45)
        assert cron.hours == (8, 10, 12, 14, 16, 18)
        assert cron.days_of_month == (1, 15)
        assert cron.days_of_week == (1, 2, 3, 4, 5)

    def test_bare_value_with_step_runs_to_field_maximum(self):
        # Vixie semantics: "5/15" in the minute field means 5-59/15.
        cron = CronExpression("5/15 * * * *")
        assert cron.minutes == (5, 20, 35, 50)

    def test_step_equal_to_the_field_range_is_accepted(self):
        # The rejection boundary is the field's range: "*/61" is rejected
        # (it would silently collapse to minute 0), "*/60" still parses.
        assert CronExpression("*/60 * * * *").minutes == (0,)

    def test_month_and_weekday_names(self):
        cron = CronExpression("0 12 * JAN-mar Mon-fri")
        assert cron.months == (1, 2, 3)
        assert cron.days_of_week == (1, 2, 3, 4, 5)

    def test_weekday_seven_means_sunday(self):
        assert CronExpression("0 0 * * 7").days_of_week == (0,)
        assert CronExpression("0 0 * * 5-7").days_of_week == (0, 5, 6)

    @pytest.mark.parametrize(
        "macro,expression",
        [
            ("@yearly", "0 0 1 1 *"),
            ("@annually", "0 0 1 1 *"),
            ("@monthly", "0 0 1 * *"),
            ("@weekly", "0 0 * * 0"),
            ("@daily", "0 0 * * *"),
            ("@midnight", "0 0 * * *"),
            ("@hourly", "0 * * * *"),
        ],
    )
    def test_macros(self, macro, expression):
        expanded = CronExpression(macro)
        reference = CronExpression(expression)
        for attr in ("minutes", "hours", "days_of_month", "months", "days_of_week"):
            assert getattr(expanded, attr) == getattr(reference, attr)

    @pytest.mark.parametrize(
        "expression",
        [
            "60 * * * *",  # minute out of range
            "* 24 * * *",  # hour out of range
            "* * 0 * *",  # day-of-month out of range
            "* * 32 * *",
            "* * * 13 *",  # month out of range
            "* * * * 8",  # day-of-week out of range
            "*/0 * * * *",  # zero step
            "*/x * * * *",  # non-numeric step
            "*/61 * * * *",  # step larger than the minute field's range
            "* */25 * * *",  # step larger than the hour field's range
            "* * */32 * *",  # step larger than the day-of-month field's range
            "* * * */13 *",  # step larger than the month field's range
            "* * * * */9",  # step larger than the day-of-week field's range
            "5-1 * * * *",  # descending range
            "* * * *",  # wrong field count
            "* * * * * *",
            "banana * * * *",
            "@fortnightly",
            "",
        ],
    )
    def test_rejects_invalid_expressions(self, expression):
        with pytest.raises(ValueError):
            CronExpression(expression)

    @pytest.mark.parametrize(
        "expression", ["0 0 30 2 *", "0 0 31 2 *", "0 0 31 4,6,9,11 *"]
    )
    def test_rejects_never_matching_expressions(self, expression):
        with pytest.raises(ValueError, match="can never match"):
            CronExpression(expression)

    def test_restricted_day_of_week_makes_any_day_reachable(self):
        # The dom/dow OR rule means Fridays satisfy this even though
        # February 30th does not exist.
        CronExpression("0 0 30 2 5")

    def test_leap_day_expression_is_valid(self):
        CronExpression("0 0 29 2 *")

    def test_str_round_trips_expression(self):
        assert str(CronExpression(" 0 3 * * * ")) == "0 3 * * *"


class TestMatches:
    def test_matches_ignores_seconds(self):
        cron = CronExpression("30 14 * * *")
        assert cron.matches(datetime(2026, 8, 13, 14, 30, 59))
        assert not cron.matches(datetime(2026, 8, 13, 14, 31))

    def test_dom_dow_or_rule(self):
        # 2026-08-13 is a Thursday, 2026-08-14 a Friday.
        cron = CronExpression("0 0 13 * 5")
        assert cron.matches(datetime(2026, 8, 13))  # the 13th
        assert cron.matches(datetime(2026, 8, 14))  # a Friday
        assert not cron.matches(datetime(2026, 8, 12))

    def test_dom_and_dow_apply_alone_when_only_one_is_restricted(self):
        assert not CronExpression("0 0 13 * *").matches(datetime(2026, 8, 14))
        assert not CronExpression("0 0 * * 5").matches(datetime(2026, 8, 13))


class TestFollowing:
    def test_next_minute(self):
        cron = CronExpression("* * * * *")
        assert cron.following(datetime(2026, 8, 13, 12, 0, 30)) == datetime(
            2026, 8, 13, 12, 1
        )

    def test_is_strictly_after_a_tick(self):
        cron = CronExpression("0 3 * * *")
        assert cron.following(datetime(2026, 8, 13, 3, 0)) == datetime(
            2026, 8, 14, 3, 0
        )

    def test_same_day_when_tick_still_ahead(self):
        cron = CronExpression("0 3 * * *")
        assert cron.following(datetime(2026, 8, 13, 1, 59)) == datetime(
            2026, 8, 13, 3, 0
        )

    def test_rolls_over_month_and_year(self):
        cron = CronExpression("0 0 1 * *")
        assert cron.following(datetime(2026, 12, 15)) == datetime(2027, 1, 1)

    def test_weekday_schedule(self):
        # 2026-08-13 is a Thursday; the next Monday is the 17th.
        cron = CronExpression("0 9 * * mon")
        assert cron.following(datetime(2026, 8, 13, 10, 0)) == datetime(
            2026, 8, 17, 9, 0
        )

    def test_leap_day_gap_spans_years(self):
        cron = CronExpression("0 0 29 2 *")
        assert cron.following(datetime(2026, 1, 1)) == datetime(2028, 2, 29)

    def test_dom_dow_or_rule(self):
        cron = CronExpression("0 0 13 * 5")
        # After the 13th, the next match is Friday the 14th, not Sep 13th.
        assert cron.following(datetime(2026, 8, 13, 0, 0)) == datetime(2026, 8, 14)

    def test_rejects_aware_datetimes(self):
        with pytest.raises(ValueError, match="naive"):
            CronExpression("* * * * *").following(datetime(2026, 8, 13, tzinfo=UTC))


class TestPrevious:
    def test_same_minute_is_inclusive(self):
        cron = CronExpression("0 3 * * *")
        assert cron.previous(datetime(2026, 8, 13, 3, 0, 45)) == datetime(
            2026, 8, 13, 3, 0
        )

    def test_earlier_same_day(self):
        cron = CronExpression("30 14 * * *")
        assert cron.previous(datetime(2026, 8, 13, 20, 0)) == datetime(
            2026, 8, 13, 14, 30
        )

    def test_previous_day_when_tick_not_yet_reached(self):
        cron = CronExpression("0 3 * * *")
        assert cron.previous(datetime(2026, 8, 13, 1, 0)) == datetime(2026, 8, 12, 3, 0)

    def test_rolls_back_month_and_year(self):
        cron = CronExpression("0 0 1 6 *")
        assert cron.previous(datetime(2026, 1, 15)) == datetime(2025, 6, 1)

    def test_leap_day_gap_spans_years(self):
        cron = CronExpression("0 0 29 2 *")
        assert cron.previous(datetime(2026, 1, 1)) == datetime(2024, 2, 29)

    def test_rejects_aware_datetimes(self):
        with pytest.raises(ValueError, match="naive"):
            CronExpression("* * * * *").previous(datetime(2026, 8, 13, tzinfo=UTC))

    @pytest.mark.parametrize(
        "expression",
        [
            "* * * * *",
            "*/5 * * * *",
            "0 3 * * *",
            "0 9 * * mon",
            "0 0 13 * 5",
            "@monthly",
        ],
    )
    def test_previous_and_following_are_consistent(self, expression):
        # For any tick t returned by following(), t is its own previous()
        # and following(t) is a later tick whose previous() is itself.
        cron = CronExpression(expression)
        tick = cron.following(datetime(2026, 8, 13, 11, 7, 13))
        assert cron.previous(tick) == tick
        later = cron.following(tick)
        assert later > tick
        assert cron.previous(later) == later
        # A probe just before a tick resolves to the preceding tick.
        assert cron.previous(later - timedelta(minutes=1)) < later
