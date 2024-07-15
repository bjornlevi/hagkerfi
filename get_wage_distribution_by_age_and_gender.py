import requests
import pandas as pd

def fetch_income_distribution():
    # Define the URL and the payload for the POST request
    url = "https://px.hagstofa.is:443/pxis/api/v1/is/Samfelag/launogtekjur/3_tekjur/1_tekjur_skattframtol/TEK01001.px"
    payload = {
        "query": [
            {
                "code": "Tekjur og skattar",
                "selection": {
                    "filter": "item",
                    "values": [
                        "1", "2", "3", "4", "5"
                    ]
                }
            },
            {
                "code": "Eining",
                "selection": {
                    "filter": "item",
                    "values": [
                        "0", "4"
                    ]
                }
            },
            {
                "code": "Kyn",
                "selection": {
                    "filter": "item",
                    "values": [
                        "1", "2"
                    ]
                }
            },
            {
                "code": "Aldur",
                "selection": {
                    "filter": "item",
                    "values": [
                        "16", "20", "25", "30", "35", "40", "45", "50", "55",
                        "60", "65", "70", "75", "80", "85+"
                    ]
                }
            },
            {
                "code": "Ár",
                "selection": {
                    "filter": "item",
                    "values": [
                        "2023"
                    ]
                }
            }
        ],
        "response": {
            "format": "json"
        }
    }

    # Send the POST request
    response = requests.post(url, json=payload)
    data = response.json()

    # Mapping dictionaries
    tekjur_og_skattar_map = {
        "1": "Atvinnutekjur",
        "2": "Fjármagnstekjur",
        "3": "Ráðstöfunartekjur",
        "4": "Aðrar tekjur",
        "5": "Skattar á greiðslugrunni"
    }

    eining_map = {
        "0": "Meðaltal",
        "4": "Fjöldi"
    }

    kyn_map = {
        "1": "Karlar",
        "2": "Konur"
    }

    aldur_map = {
        "16": "16 ára",
        "20": "20 ára",
        "25": "25 ára",
        "30": "30 ára",
        "35": "35 ára",
        "40": "40 ára",
        "45": "45 ára",
        "50": "50 ára",
        "55": "55 ára",
        "60": "60 ára",
        "65": "65 ára",
        "70": "70 ára",
        "75": "75 ára",
        "80": "80 ára",
        "85+": "85 ára og eldri"
    }

    # Extract dimensions and data
    data_values = data['data']

    # Create a dictionary to store the data
    result_data = {
        "Aldur": [],
        "Ár": [],
        "Kyn": [],
        "Fjöldi": [],
        "Atvinnutekjur": [],
        "Fjármagnstekjur": [],
        "Aðrar tekjur": [],
        "Skattar á greiðslugrunni": [],
        "Ráðstöfunartekjur": []
    }

    # Initialize a dictionary to store intermediate data
    intermediate_data = {}

    # Iterate over the data values and format them
    for item in data_values:
        key = item['key']
        values = item['values']
        
        aldur = aldur_map[key[3]]
        ar = key[4]
        kyn = kyn_map[key[2]]
        eining = eining_map[key[1]]
        tekjur_og_skattar = tekjur_og_skattar_map[key[0]]

        if (aldur, ar, kyn) not in intermediate_data:
            intermediate_data[(aldur, ar, kyn)] = {
                "Fjöldi": None,
                "Atvinnutekjur": None,
                "Fjármagnstekjur": None,
                "Aðrar tekjur": None,
                "Skattar á greiðslugrunni": None,
                "Ráðstöfunartekjur": None
            }

        if eining == "Fjöldi":
            intermediate_data[(aldur, ar, kyn)]["Fjöldi"] = values[0]
        elif eining == "Meðaltal":
            intermediate_data[(aldur, ar, kyn)][tekjur_og_skattar] = values[0]

    # Fill the result_data dictionary with the intermediate data
    for key, value in intermediate_data.items():
        aldur, ar, kyn = key
        result_data["Aldur"].append(aldur)
        result_data["Ár"].append(ar)
        result_data["Kyn"].append(kyn)
        result_data["Fjöldi"].append(value["Fjöldi"])
        result_data["Atvinnutekjur"].append(value["Atvinnutekjur"])
        result_data["Fjármagnstekjur"].append(value["Fjármagnstekjur"])
        result_data["Aðrar tekjur"].append(value["Aðrar tekjur"])
        result_data["Skattar á greiðslugrunni"].append(value["Skattar á greiðslugrunni"])
        result_data["Ráðstöfunartekjur"].append(value["Ráðstöfunartekjur"])

    # Create a DataFrame
    df = pd.DataFrame(result_data)

    # Save the DataFrame to a CSV file
    df.to_csv("data/income_by_age_and_gender.csv", index=False)
    print("Data saved to data/income_by_age_and_gender.csv")
    print(df)

# Fetch and save the income distribution data
fetch_income_distribution()
