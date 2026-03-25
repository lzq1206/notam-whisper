#!/usr/bin/env python3
import datetime

import fetch_notams


def test_is_in_time_window_accepts_empty_window():
    assert fetch_notams._is_in_time_window('', '') is True


def test_is_in_time_window_filters_by_range():
    now = datetime.datetime.now(datetime.timezone.utc)

    valid_from = (now + datetime.timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    valid_to = (now + datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    assert fetch_notams._is_in_time_window(valid_from, valid_to) is True

    too_far_from_now = (now + datetime.timedelta(days=fetch_notams.MAX_FUTURE_DAYS + 1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    assert fetch_notams._is_in_time_window(too_far_from_now, valid_to) is False

    expired_to = (now - datetime.timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    assert fetch_notams._is_in_time_window('', expired_to) is False


if __name__ == '__main__':
    test_is_in_time_window_accepts_empty_window()
    test_is_in_time_window_filters_by_range()
    print('test_fetch_notams_time_window.py passed')
