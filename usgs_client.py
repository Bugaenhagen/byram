#usgs_client.py
#USGS NWIS data pipeline - Byram River discharge
#Port Chester Yacht Club storm event analysis
#Delaware Engineering, D.P.C

import requests
import pandas

#Defines the fetchStream function to request data from USGS API
def fetchStream(stationId, startDate, endDate, parameterCd, columnName):
    # Base URL for USGS National Water Information System instantaneous values.
    url = "https://waterservices.usgs.gov/nwis/iv/"

    #Query parameters - Byram River at Pemberwick (01212500), parameter code, Date range.
    #parameterCd 00060 = stream discharge (cfs), 00065 = gauge height (ft), 00045 = precipitation (in)
    params = {
        "sites": stationId,
        "parameterCd": parameterCd,
        "startDT": startDate,
        "endDT": endDate,
        "format": "json"
    }

    #Make the HTTP request to USGS API
    response = requests.get(url, params=params)

    #Parse JSON response into Python dictionary
    data = response.json()

    #Navigates nested JSON structure to individual discharge readings.
    readings = data["value"]["timeSeries"][0]["values"][0]["value"]

    #Empty list to collect readings before converting to DataFrame
    records = []

    for reading in readings:
        datetime = reading["dateTime"]
        value = float(reading["value"])
        qualifier = reading["qualifiers"]
        records.append({"datetime": datetime, columnName: value, "qualifiers":qualifier})

    #Convert records list to pandas DataFrame
    df = pandas.DataFrame(records)

    return df

def buildStormDataset(stormName, startDate, endDate):
    df_discharge = fetchStream("01212500", startDate, endDate,
    "00060", "discharge_cfs")
    df_discharge = df_discharge.rename(columns={"qualifiers": "qualifiers_discharge"})
    df_gauge = fetchStream("01212500", startDate, endDate, "00065",
                           "height_ft")
    df_gauge = df_gauge.rename(columns={"qualifiers":"qualifiers_gauge"})
    df_precip = fetchStream("410138073394201", startDate, endDate, "00045",
                            "precipitation_in")
    df_precip = df_precip.rename(columns={"qualifiers": "qualifiers_precip"})
    df_merged = pandas.merge(df_discharge, df_gauge, on="datetime")
    df_merged = pandas.merge(df_merged, df_precip, on="datetime")
    df_merged.to_csv(path_or_buf=stormName + "_streamData.csv", index=False)

if __name__ == "__main__":

   buildStormDataset("sandy", "2012-10-27", "2012-10-31")
   buildStormDataset("ida", "2021-08-29", "2021-09-03")
   buildStormDataset("ophelia", "2023-09-27", "2023-10-01")