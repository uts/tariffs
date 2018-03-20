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
        working_data_store = pandas.HDFStore('dummy_data_store.h5')
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
                        "meter": "imported energy (kwh)",
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
                        "meter": "imported energy (kwh)",
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
                        "rate": 0.1,
                        "meter": "imported power (kw)",
                        "type": "demand"
                    }
                ],
                "service": "electricity",
                "demand_window": "15min",
                "billing_period": "monthly"
            }, Tariff
        )
        return demand_tariff

    def test_flat_tariff(self,series_data_df):
        expected_bill = 1339.20
        meter_data_df = pandas.DataFrame([])
        meter_data_df['imported energy (kwh)'] = series_data_df['load_series_flat']
        with open('tariff_test_flat.json') as f:
            test_tariff = json_codec.load(f, Tariff)
        actual_bill = test_tariff.apply(meter_data_df)
        assert actual_bill == pytest.approx(expected_bill)

    def test_block_tariff(self,series_data_df, block_tariff):
        expected_bill = 162.549
        meter_data_df = pandas.DataFrame([])
        meter_data_df['imported energy (kwh)'] = series_data_df['load_series_real']
        actual_bill = block_tariff.apply(meter_data_df)
        assert actual_bill == pytest.approx(expected_bill)

    def test_tou_tariff(self, series_data_df):
        expected_bill = 8928
        meter_data_df = pandas.DataFrame([])
        meter_data_df['imported energy (kwh)'] = series_data_df['load_series_flat']
        with open('tariff_test_tou.json') as f:
            test_tariff = json_codec.load(f, Tariff)
        actual_bill = test_tariff.apply(meter_data_df)
        assert actual_bill == pytest.approx(expected_bill)

    def test_demand_tariff(self, demand_tariff, series_data_df):
        expected_bill = 300*0.1
        meter_data_df = pandas.DataFrame([])
        meter_data_df['imported energy (kwh)'] = series_data_df['load_series_bimodal']
        meter_data_df['imported power (kw)'] = series_data_df['load_series_bimodal']*60
        actual_bill = demand_tariff.apply(meter_data_df)
        assert actual_bill == expected_bill

    def test_multi_tou_weekday_tariff(self, series_data_df):
        expected_bill = 17486.60175860
        meter_data_df = pandas.DataFrame([])
        meter_data_df['imported energy (kwh)'] = series_data_df['load_series_bimodal']
        with open('tariff_multi_tou_weekday.json') as f:
            test_tariff = json_codec.load(f, Tariff)
        actual_bill = test_tariff.apply(meter_data_df)
        assert actual_bill == pytest.approx(expected_bill)


        # def test_demand__block_tariff(self, demand_tariff, meter_data):
        #     expected_bill = 1.0
        #     actual_bill = demand_tariff.apply(meter_data)
        #     assert actual_bill == expected_bill


    # def test_scheduled_tariff(self, scheduled_tariff, series_data_df):
    #     expected_bill = 57106330.2
    #     meter_data_df = pandas.DataFrame([])
    #     meter_data_df['imported energy (kwh)'] = series_data_df['load_series_flat']
    #     actual_bill = scheduled_tariff.apply(meter_data_df)
    #     assert actual_bill == expected_bill
    #
    # def test_seasonal_tariff(self, seasonal_tariff, meter_data):
    #     expected_bill = 1.0
    #     actual_bill = seasonal_tariff.apply(meter_data)
    #     assert actual_bill == expected_bill
    #

    #
    #
    #
    # def test_supply_payment(self, supply_payment_tariff, meter_data):
    #     expected_bill = 1.0
    #     actual_bill = supply_payment_tariff.apply(meter_data)
    #     assert actual_bill == expected_bill
    #
