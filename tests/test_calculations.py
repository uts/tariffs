from tariffs.tariff import Tariff
import pytest

from odin.codecs import dict_codec
from odin.codecs import json_codec


import pandas
import datetime



parser = lambda t: datetime.datetime.strptime(t, '%d/%m/%Y %H:%M')



class TestTariff(object):

    @pytest.fixture
    def meter_data(self):
        with open('./fixtures/test_load_data.csv') as f:
            meter_data = pandas.read_csv(f, index_col='datetime', parse_dates=True, infer_datetime_format=True,
                                         date_parser=parser)
        return meter_data

    @pytest.fixture
    def series_data_df(self):
        working_data_store = pandas.HDFStore('./dummy_data_store.h5')
        series_data_df = working_data_store.select('time_series_data')
        working_data_store.close()
        return series_data_df

    @pytest.fixture
    def block_tariff(self):
        block_tariff = dict_codec.load(
            {
                "charges": [
                    {
                        "type": "consumption",
                        "rate_bands": [
                            {
                                "limit": 10,
                                "rate": 0.10
                            },
                            {
                                "limit": 400,
                                "rate": 0.15
                            },
                            {
                                "rate": 0.30
                            }
                        ]
                    }
                ],
                "service": "electricity",
                "consumption_unit": "kWh",
                "demand_unit": "kVA",
                "billing_period": "monthly"
            }, Tariff
        )
        return block_tariff

    @pytest.fixture
    def seasonal_tariff(self):
        seasonal_tariff = dict_codec.load(
            {
                "charges": [
                    {
                        "rate": 1.0,
                        "season": {
                            "name": "summer",
                            "from_month": 1,
                            "from_day": 1,
                            "to_month": 3,
                            "to_day": 31
                        }
                    },
                    {
                        "rate": 1.0,
                        "season": {
                            "name": "winter",
                            "from_month": 4,
                            "from_day": 1,
                            "to_month": 12,
                            "to_day": 31
                        }
                    }
                ],
                "service": "electricity",
                "consumption_unit": "kWh",
                "demand_unit": "kVA",
                "billing_period": "monthly"
            }, Tariff
        )
        return seasonal_tariff

    @pytest.fixture
    def tou_tariff(self):
        tou_tariff = dict_codec.load(
            {
                "charges": [
                    {
                        "rate": 1.0,
                        "time": {
                            "name": "peak",
                            "periods": [
                                {
                                    "from_weekday": 0,
                                    "to_weekday": 4,
                                    "from_hour": 14,
                                    "to_hour": 19,
                                }
                            ]
                        }
                    },
                    {
                        "rate": 1.0,
                        "time": {
                            "name": "shoulder",
                            "periods": [
                                {
                                    "from_weekday": 0,
                                    "to_weekday": 4,
                                    "from_hour": 10,
                                    "to_hour": 13,
                                },
                                {
                                    "from_weekday": 0,
                                    "to_weekday": 4,
                                    "from_hour": 20,
                                    "to_hour": 21,
                                }
                            ]
                        }
                    },
                    {
                        "rate": 1.0,
                        "time": {
                            "name": "off-peak",
                            "periods": [
                                {
                                    "from_weekday": 0,
                                    "to_weekday": 4,
                                    "from_hour": 0,
                                    "from_minute": 0,
                                    "to_hour": 9,
                                    "to_minute": 59
                                },
                                {
                                    "from_weekday": 0,
                                    "to_weekday": 4,
                                    "from_hour": 22,
                                    "from_minute": 0,
                                    "to_hour": 23,
                                    "to_minute": 59
                                },
                                {
                                    "from_weekday": 5,
                                    "to_weekday": 6
                                }
                            ]
                        }
                    }
                ],
                "service": "electricity",
                "consumption_unit": "kWh",
                "demand_unit": "kVA",
                "billing_period": "monthly"
            }, Tariff
        )
        return tou_tariff

    @pytest.fixture
    def scheduled_tariff(self):
        scheduled_tariff = dict_codec.load(
            {
                "charges": [
                    {
                        "meter": "electricity_imported",
                        "rate_schedule": [
                            {
                                "datetime":"2018-01-01T00:00:00Z",
                                "rate": 1.0
                            },
                            {
                                "datetime":"2018-06-01T00:30:00Z",
                                "rate": 1.0
                            },
                            {
                                "datetime": "2018-12-31T01:00:00Z",
                                "rate": 1.0
                            }
                        ]
                    }
                ],
                "service": "electricity",
                "energy_unit": "kWh",
                "demand_unit": "kVA"
            }, Tariff
        )
        return scheduled_tariff

    @pytest.fixture
    def supply_payment_tariff(self):
        supply_payment_tariff = dict_codec.load(
            {
                "charges": [
                    {
                        "rate": -1.0,
                        "meter": "electricity_exported"
                    }
                ],
                "service": "electricity"
            }, Tariff
        )
        return supply_payment_tariff

    @pytest.fixture
    def demand_tariff(self):
        demand_tariff = dict_codec.load(
            {
                "charges": [
                    {
                        "rate": 1,
                        "type": "demand"
                    }
                ],
                "service": "electricity",
                "demand_window": "15min",
                "billing_period": "monthly"
            }, Tariff
        )
        return demand_tariff


    #Flat tariff tests

    def test_flat_tariff(self, series_data_df):
        expected_bill = 1339.20   # = (8928kWh * 0.15#$/kWh)
        meter_data_df = pandas.DataFrame([])
        meter_data_df['imported energy (kwh)'] = series_data_df['load_series_flat']
        with open('tariff_test_flat.json') as f:
            test_tariff = json_codec.load(f, Tariff)
        actual_bill = test_tariff.apply(meter_data_df)
        assert actual_bill == pytest.approx(expected_bill)

    # TOU tariff tests

    def test_tou_tariff_with_fixture_meter_data(self, tou_tariff, meter_data):
        expected_bill = 35040.0
        actual_bill = tou_tariff.apply(meter_data)
        assert actual_bill == expected_bill

    def test_tou_tariff_with_imported_series_data(self, tou_tariff, series_data_df):
        expected_bill = series_data_df['load_series_flat'].sum()
        meter_data_df = pandas.DataFrame([])
        meter_data_df['electricity_imported'] = series_data_df['load_series_flat']
        actual_bill = tou_tariff.apply(meter_data_df)
        assert actual_bill == pytest.approx(expected_bill)

    def test_imported_tou_tariff_with_imported_series_data(self, series_data_df):
        expected_bill = series_data_df['load_series_flat'].sum()
        meter_data_df = pandas.DataFrame([])
        meter_data_df['imported energy (kwh)'] = series_data_df['load_series_flat']
        with open('tariff_test_tou.json') as f:
            test_tariff = json_codec.load(f, Tariff)
        actual_bill = test_tariff.apply(meter_data_df)
        assert actual_bill == pytest.approx(expected_bill)

    def test_imported_simple_tou_tariff_with_imported_series_data(self, series_data_df):
        expected_bill = series_data_df['load_series_flat'].sum()
        meter_data_df = pandas.DataFrame([])
        meter_data_df['imported energy (kwh)'] = series_data_df['load_series_flat']
        with open('tariff_test_simple_tou.json') as f:
            test_tariff = json_codec.load(f, Tariff)
        actual_bill = test_tariff.apply(meter_data_df)
        assert actual_bill == pytest.approx(expected_bill)

    # Scheduled tariff tests
    def test_scheduled_tariff_with_fixture_meter_data(self, scheduled_tariff, meter_data):
        expected_bill = 35040.0
        actual_bill = scheduled_tariff.apply(meter_data)
        assert actual_bill == expected_bill

    # Block tariff tests
    def test_block_tariff_with_fixture_meter_data(self, block_tariff, meter_data):
        expected_bill = 0
        expected_bill = expected_bill + ((2976 - 400) * 0.3 + (400  -10) * 0.15 + 10 * 0.1) * 7  # 7 * 31 day months in a year
        expected_bill = expected_bill + ((2880 - 400) * 0.3 + (400 - 10) * 0.15 + 10 * 0.1) * 4  # 4 *30 day months in a year
        expected_bill = expected_bill + ((2688 - 400) * 0.3 + (400 - 10) * 0.15 + 10 * 0.1) * 1  # 1 * 28 day months in year
        actual_bill = block_tariff.apply(meter_data)
        assert actual_bill == pytest.approx(expected_bill)

    def test_block_tariff_with_imported_series_data(self,series_data_df, block_tariff):
        total_usage = series_data_df['load_series_real'].sum()
        expected_bill = (total_usage-400)*0.3 + (400-10)*0.15 + 10*0.1
        meter_data_df = pandas.DataFrame([])
        meter_data_df['electricity_imported'] = series_data_df['load_series_real']
        actual_bill = block_tariff.apply(meter_data_df)
        assert actual_bill == pytest.approx(expected_bill)

    # Demand tariff tests

    def test_demand_tariff_with_fixture_meter_data(self, demand_tariff, meter_data):
        expected_bill = 12
        actual_bill = demand_tariff.apply(meter_data)
        assert actual_bill == expected_bill

    def test_block_demand_tariff_with_imported_series_data(self, series_data_df):
        #Max usage is 75kWh in a 15 minute period (ie 300kW), total usage is 133920kWh
        expected_bill = (300-120)*28.45

        meter_data_df = pandas.DataFrame([])
        meter_data_df['imported energy (kwh)'] = series_data_df['load_series_bimodal']
        meter_data_df['imported power (kw)'] = series_data_df['load_series_bimodal']*60
        with open('tariff_block_demand.json') as f:
            test_tariff = json_codec.load(f, Tariff)
        actual_bill = test_tariff.apply(meter_data_df)
        assert actual_bill == pytest.approx(expected_bill)

    # Seasonal tariff tests

    def test_seasonal_tariff_with_fixture_meter_data(self, seasonal_tariff, meter_data):
        expected_bill = 35040.0
        actual_bill = seasonal_tariff.apply(meter_data)
        assert actual_bill == expected_bill

    # Supply tariff tests

    def test_supply_payment_with_fixture_meter_data(self, supply_payment_tariff, meter_data):
        expected_bill = -35040.0
        actual_bill = supply_payment_tariff.apply(meter_data)
        assert actual_bill == expected_bill

    # Combination tariff tests
    def test_multi_tou_weekday_tariff(self, series_data_df):
        expected_bill = 15432.56
        meter_data_df = pandas.DataFrame([])
        meter_data_df['imported energy (kwh)'] = series_data_df['load_series_bimodal']
        meter_data_df['imported power (kw)'] = series_data_df['load_series_bimodal']*60
        with open('tariff_multi_tou_weekday.json') as f:
            test_tariff = json_codec.load(f, Tariff)
        actual_bill = test_tariff.apply(meter_data_df)
        assert actual_bill == pytest.approx(expected_bill)

    def test_consumption_with_block_demand_tariff_with_imported_series_data(self, series_data_df):
        # Max usage is 150kWh in a 30 minute period (ie 300kW), total usage is 133920kWh
        demand_bill = (300 - 120) * 0.001
        consumption_bill = 10 * 133920
        expected_bill = demand_bill + consumption_bill

        meter_data_df = pandas.DataFrame([])
        meter_data_df['imported energy (kwh)'] = series_data_df['load_series_bimodal']
        meter_data_df['imported power (kw)'] = series_data_df['load_series_bimodal'] * 60
        with open('tariff_consumption_with_block_demand.json') as f:
            test_tariff = json_codec.load(f, Tariff)
        actual_bill = test_tariff.apply(meter_data_df)
        assert actual_bill == pytest.approx(expected_bill)


    def test_south_australia_with_imported_series_data(self, series_data_df):
        # Max usage is 150kWh in a 30 minute period (ie 300kW), total usage is 133920kWh
        demand_bill = (300 - 120) * 28.45
        consumption_bill = 0.117545 * 133920
        expected_bill = demand_bill + consumption_bill

        meter_data_df = pandas.DataFrame([])
        meter_data_df['imported energy (kwh)'] = series_data_df['load_series_bimodal']
        meter_data_df['imported power (kw)'] = series_data_df['load_series_bimodal'] * 60
        with open('tariff_south_australia.json') as f:
            test_tariff = json_codec.load(f, Tariff)
        actual_bill = test_tariff.apply(meter_data_df)
        assert actual_bill == pytest.approx(expected_bill)

