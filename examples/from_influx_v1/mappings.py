sides = {
    "LWS": {
        "BUILDING_DISTRIBUTION": [
            "IL1",
            "IL2",
            "IL3",
            "P",
            "PL1",
            "PL2",
            "PL3",
            "Q",
            "QL1",
            "QL2",
            "QL3",
            "S",
            "SL1",
            "SL2",
            "SL3",
            "UL1N",
            "UL2N",
            "UL3N",
        ],
        "OUTDOOR_TEMPERATURE": [
            "fCnt",
            "object_frameCounter",
            "object_hardwareError",
            "object_lowBattery",
            "object_temperatureExtern",
            "object_temperatureIntern",
            "rxInfo_0_loRaSNR",
            "rxInfo_0_location_altitude",
            "rxInfo_0_location_latitude",
            "rxInfo_0_location_longitude",
            "rxInfo_0_rssi",
            "txInfo_dr",
            "txInfo_frequency",
        ],
        "CHP_50": [
            "IL1",
            "IL2",
            "IL3",
            "P",
            "PL1",
            "PL2",
            "PL3",
            "Q",
            "QL1",
            "QL2",
            "QL3",
            "S",
            "SL1",
            "SL2",
            "SL3",
            "UL1N",
            "UL2N",
            "UL3N",
            "FLOW_1",
            "POWER_1",
            "TEMP_R_1",
            "TEMP_V_1",
        ],
        "DISTRICT_HEATING": ["FLOW_2", "POWER_2", "TEMP_R_2", "TEMP_V_2"],
        "DRINKING_WATER": ["FLOW_2", "POWER_2", "TEMP_R_2", "TEMP_V_2"],
        "STORAGE_TANK": ["FLOW_1", "POWER_1", "TEMP_R_1", "TEMP_V_1"],
    },
    "WHITESAIL": {
        "BOILER": ["FLOW_2", "POWER_2", "TEMP_R_2", "TEMP_V_2"],
        "DRINKING_WATER": ["FLOW_1", "POWER_1", "TEMP_R_1", "TEMP_V_1"],
        "HEATING1": ["FLOW_1", "POWER_1", "TEMP_R_1", "TEMP_V_1"],
        "HEATING2": ["FLOW_2", "POWER_2", "TEMP_R_2", "TEMP_V_2"],
        "OFFICE_DISTRIBUTION": [
            "IL1",
            "IL2",
            "IL3",
            "P",
            "PL1",
            "PL2",
            "PL3",
            "Q",
            "QL1",
            "QL2",
            "QL3",
            "S",
            "SL1",
            "SL2",
            "SL3",
            "UL1N",
            "UL2N",
            "UL3N",
        ],
        "OUTDOOR_TEMPERATURE": [
            "fCnt",
            "object_frameCounter",
            "object_hardwareError",
            "object_lowBattery",
            "object_temperatureExtern",
            "object_temperatureIntern",
            "rxInfo_0_loRaSNR",
            "rxInfo_0_location_altitude",
            "rxInfo_0_location_latitude",
            "rxInfo_0_location_longitude",
            "rxInfo_0_rssi",
            "txInfo_dr",
            "txInfo_frequency",
        ],
        "SOLAR": ["FLOW_1", "POWER_1", "TEMP_R_1", "TEMP_V_1"],
        "STORAGE_TANK": ["FLOW_2", "POWER_2", "TEMP_R_2", "TEMP_V_2"],
    },
}

time_ranges = {
    "LWS": {"start": "2020-01-01T00:00:00Z", "end": "2020-12-05T00:00:00Z"},
    "WHITESAIL": {"start": "2020-01-01T00:00:00Z", "end": "2020-10-01T00:00:00Z"},
}

target_measurements = [
    "T_S_25",
    "T_S_26",
    "T_S_28",
    "T_S_29",
    "T_S_30",
    "T_S_31",
    "T_S_32",
    "T_S_33",
    "T_S_34",
    "T_S_35",
    "T_S_36",
    "T_S_37",
    "T_S_38",
    "T_S_39",
    "T_S_40",
    "T_S_41",
    "T_S_42",
    "T_S_43",
    "T_S_44",
    "T_S_45",
    "T_S_46",
    "T_S_47",
    "T_S_48",
    "T_S_49",
    "T_S_50",
]

measurements_with_extern_temperature = [
    "T_HA_02",
    "T_S_01",
    "T_S_02",
    "T_S_03",
    "T_S_04",
    "T_S_05",
    "T_S_06",
    "T_S_07",
    "T_S_08",
    "T_S_09",
    "T_S_10",
    "T_S_11",
    "T_S_25",
]
