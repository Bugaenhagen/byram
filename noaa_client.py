#noaa_client.py
#NOAA Client - Kings Point and Bridgeport Tidal Data
#Port Chester Yacht Club storm event analysis
#Delaware Engineering, D.P.C

import requests
import pandas

NOAA_BASE_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"

#Defines the fetchTide function to request data from NOAA API
def fetchTide(stationID, startDate, endDate, product, columnName):
    url = NOAA_BASE_URL

    startDate = startDate.replace("-", "")
    endDate = endDate.replace("-", "")

    # Query parameters - station ID, product, date range
    # Products: water_level, predictions, wind, air_pressure, air_temperature
    # Datum: MLLW (Mean Lower Low Water) - standard tidal reference
    # Units: english (feet, knots)
    params = {
        "station": stationID,
        "product": product,
        "begin_date": startDate,
        "end_date": endDate,
        "datum": "MLLW",
        "time_zone": "lst_ldt",
        "units": "english",
        "format": "json"
    }

    #Make the HTTP request to NOAA API
    response = requests.get(url, params=params)

    #Parse JSON response into Python dictionary
    data = response.json()

    #Navigate JSON structure.
    readings = data.get("data") or data.get("predictions")

    #Empty list to collect readings.
    records = []

    for reading in readings:
        timestamp = reading["t"]
        waterLevel = float(reading["v"])
        sigma = reading.get("s")
        dataFlags = reading.get("f")
        quality = reading.get("q")
        records.append({
            "timestamp":timestamp,
            columnName: waterLevel,
            "sigma": sigma,
            "quality":quality,
            "flags":dataFlags
        })

    #Convert records list to pandas DataFrame
    df = pandas.DataFrame(records)

    return df

def fetchWind(stationID, startDate, endDate):
    url = NOAA_BASE_URL

    startDate = startDate.replace("-", "")
    endDate = endDate.replace("-", "")

    params = {
        "station":  stationID,
        "begin_date": startDate,
        "end_date": endDate,
        "product": "wind",
        "time_zone": "lst_ldt",
        "units": "english",
        "format": "json"
    }

    response = requests.get(url, params=params)
    data = response.json()
    readings = data["data"]
    records = []

    for reading in readings:
        timestamp = reading["t"]
        windSpeed = float(reading["s"]) if reading["s"] != "" else None
        windDirection = float(reading["d"]) if reading["d"] != "" else None
        compass = reading["dr"]
        windGusts = float(reading["g"]) if reading["g"] != "" else None
        dataFlags = reading["f"]
        records.append({
            "timestamp":timestamp,
            "wind_speed_knots":windSpeed,
            "wind_compass": compass,
            "gust_knots": windGusts,
            "wind_direction_deg": windDirection,
            "flags": dataFlags
        })
    df = pandas.DataFrame(records)

    return df

def fetchPressure(stationID, startDate, endDate):
    url = NOAA_BASE_URL

    startDate = startDate.replace("-", "")
    endDate = endDate.replace("-", "")

    params = {
        "station": stationID,
        "begin_date": startDate,
        "end_date": endDate,
        "product": "air_pressure",
        "time_zone": "lst_ldt",
        "units": "english",
        "format": "json"
    }

    response = requests.get(url, params=params)
    data = response.json()
    readings = data["data"]
    records = []

    for reading in readings:
        timestamp = reading["t"]
        barometricPressure = float(reading["v"]) if reading["v"] != "" else None
        dataFlags = reading["f"]
        records.append({
            "timestamp":timestamp,
            "barPressure_millibars":barometricPressure,
            "data_flags": dataFlags
        })

    df = pandas.DataFrame(records)

    return df

def buildCoastalDataset(stormName, stationID, startDate, endDate):
    df_tide = fetchTide(stationID, startDate, endDate, "water_level",
                        columnName="recorded_wl_ft")
    df_prediction = fetchTide(stationID, startDate, endDate, "predictions",
                              columnName="predicted_wl_ft")
    df_tide = df_tide.rename(columns={"sigma": "sigma_recorded", "quality": "quality_recorded",
                                      "flags": "flags_recorded"})
    df_prediction = df_prediction.rename(columns={"sigma": "sigma_predicted", "quality": "quality_predicted",
                                                  "flags": "flags_predicted"})
    df_wind = fetchWind(stationID, startDate, endDate)
    df_pressure = fetchPressure(stationID, startDate, endDate)

    df_merged = pandas.merge(df_tide, df_prediction, on="timestamp")
    df_merged["surge_residual_ft"] = df_merged["recorded_wl_ft"] - df_merged["predicted_wl_ft"]
    df_merged = pandas.merge(df_merged, df_wind, on="timestamp")
    df_merged = pandas.merge(df_merged, df_pressure, on="timestamp")
    df_merged.to_csv(path_or_buf=stormName + "_coastalData.csv", index=False)

if __name__ == "__main__":

    buildCoastalDataset("kingsPointSandy","8516945","2012-10-27", "2012-10-31")
    buildCoastalDataset("kingsPointsIda","8516945","2021-08-29",
                      "2021-09-03")
    buildCoastalDataset("kingsPointOphelia","8516945","2023-09-27",
                      "2023-10-01")
    buildCoastalDataset("bridgeportSandy","8467150", "2012-10-27",
                      "2012-10-31")
    buildCoastalDataset("bridgeportIda","8467150", "2021-08-29",
                      "2021-09-03")
    buildCoastalDataset("bridgeportOphelia","8467150", "2023-09-27",
                      "2023-10-01")