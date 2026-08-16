from unittest.mock import MagicMock

import pandas as pd
from weather_producer import HOURLY_VARS, poll_weather


class FakeVariable:
    def __init__(self, value):
        self._value = value

    def Value(self):
        return self._value


class FakeCurrent:
    def __init__(self, time_unix, values):
        self._time_unix = time_unix
        self._values = values

    def Time(self):
        return self._time_unix

    def Variables(self, i):
        return FakeVariable(self._values[i])


class FakeResponse:
    def __init__(self, current):
        self._current = current

    def Current(self):
        return self._current


def test_poll_weather_averages_multi_station_zones(monkeypatch):
    monkeypatch.setenv("FORECAST_API", "https://example.test/forecast")

    zones = pd.DataFrame({
        "zone_id": ["AEP", "AEP", "COMED"],
        "lat": [40.0, 41.0, 42.0],
        "lon": [-80.0, -81.0, -82.0],
    })

    # values order matches HOURLY_VARS: temperature, precipitation, wind_speed, cloud_cover
    responses = [
        FakeResponse(FakeCurrent(1000, [10.0, 0.0, 5.0, 20.0])),
        FakeResponse(FakeCurrent(2000, [20.0, 1.0, 7.0, 30.0])),
        FakeResponse(FakeCurrent(1500, [15.0, 0.5, 6.0, 25.0])),
    ]
    openmeteo = MagicMock()
    openmeteo.weather_api.return_value = responses

    result = poll_weather(openmeteo, zones)

    openmeteo.weather_api.assert_called_once_with(
        "https://example.test/forecast",
        params={
            "latitude": "40.0,41.0,42.0",
            "longitude": "-80.0,-81.0,-82.0",
            "current": HOURLY_VARS,
            "timezone": "UTC",
        },
    )

    by_zone = {row["zone"]: row for row in result}

    aep = by_zone["AEP"]
    assert aep["time"] == pd.Timestamp(1000, unit="s", tz="UTC")
    assert aep["temperature"] == 15.0
    assert aep["precipitation"] == 0.5
    assert aep["wind_speed"] == 6.0
    assert aep["cloud_cover"] == 25.0

    comed = by_zone["COMED"]
    assert comed["time"] == pd.Timestamp(1500, unit="s", tz="UTC")
    assert comed["temperature"] == 15.0
    assert comed["precipitation"] == 0.5
    assert comed["wind_speed"] == 6.0
    assert comed["cloud_cover"] == 25.0
