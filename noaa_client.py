#noaa_client.py
#NOAA Client - Kings Point and Bridgeport Tidal Data
#Port Chester Yacht Club storm event analysis
#Delaware Engineering, D.P.C

import requests
import pandas


#Defines the fetchTide function to request data from NOAA API
def fetchTide(stationID, startDate, endDate, product, columnName):
    url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"

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
        records.append({"timestamp":timestamp, columnName: waterLevel, "sigma": sigma,
                        "quality":quality,"flags":dataFlags})

        #Convert records list to pandas DataFrame
    df = pandas.DataFrame(records)

    return df

def buildTidalDataset(stormName, startDate, endDate):
    df_tide = fetchTide("8516945", startDate, endDate, "water_level",
                        columnName="recorded_wl_ft")
    df_prediction = fetchTide("8516945", startDate, endDate, "predictions",
                              columnName="predicted_wl_ft")
    df_tide = df_tide.rename(columns={"sigma": "sigma_recorded", "quality": "quality_recorded",
                                      "flags": "flags_recorded"})
    df_prediction = df_prediction.rename(columns={"sigma": "sigma_predicted", "quality": "quality_predicted",
                                                  "flags": "flags_predicted"})
    df_merged = pandas.merge(df_tide, df_prediction, on="timestamp")
    df_merged["surge_residual_ft"] = df_merged["recorded_wl_ft"] - df_merged["predicted_wl_ft"]
    df_merged.to_csv(path_or_buf=stormName + "_tideData.csv", index=False)


if __name__ == "__main__":

    buildTidalDataset("sandy", "2012-10-27", "2012-10-31")
    buildTidalDataset("ida", "2021-08-29", "2021-09-03")
    buildTidalDataset("ophelia", "2023-09-27", "2023-10-01")