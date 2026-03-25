#!/usr/bin/env python3
import fetch_notams


def test_is_in_time_window_uses_valid_utc_now():
    assert fetch_notams._is_in_time_window('', '') is True


if __name__ == '__main__':
    test_is_in_time_window_uses_valid_utc_now()
    print('test_fetch_notams_time_window.py passed')
