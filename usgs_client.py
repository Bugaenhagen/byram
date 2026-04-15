#usgs_client.py
#USGS NWIS data pipeline - Byram River discharge
#Port Chester Yacht Club storm event analysis
#Delaware Engineering, D.P.C

import requests
import pandas

#Defines the fetchStream function to request data from USGS API
def fetchStream(stationId, startDate, endDate):
    # Base URL for USGS National Water Information System instantaneous values.
    url = "https://waterservices.usgs.gov/nwis/iv/"

    #Query parameters - Byram River at Pemberwick (01212500), discharge (00060), Date range.
    #parameterCd 00060 = stream discharge in cubic feet per second
    params = {
        "sites": stationId,
        "parameterCd": "00060",
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
        records.append({"datetime": datetime, "discharge": value, "qualifiers":qualifier})

    #Convert records list to pandas DataFrame
    df = pandas.DataFrame(records)

    return df

if __name__ == "__main__":
    df = fetchStream("01212500", "2021-08-29", "2021-09-03")

    #Write DataFrame to CSV - index=False omits row numbers from input.
    df.to_csv("idaStreamData.csv", index=False)

    df = fetchStream("01212500", "2012-10-27", "2012-10-31")
    df.to_csv(path_or_buf="sandyStreamData.csv", index=False)

    df = fetchStream("01212500", "2023-09-27", "2023-10-01")
    df.to_csv(path_or_buf="opheliaStreamData.csv", index=False)