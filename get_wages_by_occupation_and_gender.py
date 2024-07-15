import requests
import pandas as pd

def fetch_wage_distribution():
    # Define the URL and the payload for the POST request
    url = "https://px.hagstofa.is:443/pxis/api/v1/is/Samfelag/launogtekjur/1_laun/1_laun/VIN02002.px"
    payload = {
        "query": [
            {
                "code": "Ár",
                "selection": {
                    "filter": "item",
                    "values": [
                        "2023"
                    ]
                }
            },
            {
                "code": "Launþegahópur",
                "selection": {
                    "filter": "item",
                    "values": [
                        "1",
                        "2"
                    ]
                }
            },
            {
                "code": "Starfsstétt",
                "selection": {
                    "filter": "item",
                    "values": [
                        "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"
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
                "code": "Laun og vinnutími",
                "selection": {
                    "filter": "item",
                    "values": [
                        "3"
                    ]
                }
            },
            {
                "code": "Eining",
                "selection": {
                    "filter": "item",
                    "values": [
                        "0"
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
    launthagarhopur_map = {
        "1": "Starfsmenn í opinberum geira",
        "2": "Starfsmenn í almennum vinnumarkaði"
    }

    starfsstett_map = {
        "1": "Stjórnendur (1)",
        "2": "Sérfræðingar (2)",
        "3": "Tæknar og sérmenntað starfsfólk (3)",
        "4": "Skrifstofufólk (4)",
        "5": "Þjónustu-, sölu- og afgreiðslufólk (5)",
        "6": "Iðnaðarmenn og sérhæft iðnverkafólk (6)",
        "7": "Véla- og vélgæslufólk (7)",
        "8": "Ósérhæft starfsfólk (8)",
        "9": "Annar iðnaður (9)",
        "10": "Aðrir sérhæfðir iðnaðarmenn (10)"
    }

    kyn_map = {
        "1": "Karlar",
        "2": "Konur"
    }

    # Extract dimensions and data
    dimensions = data['columns']
    data_values = data['data']

    # Create a list to store the formatted rows
    rows = []

    # Iterate over the data values and format them
    for item in data_values:
        key = item['key']
        values = item['values']
        
        row = [
            key[0],  # Year
            launthagarhopur_map[key[1]],  # Launþegahópur
            starfsstett_map[key[2]],  # Starfsstétt
            kyn_map[key[3]],  # Kyn
            values[0] if len(values) > 0 else None  # Meðaltal
        ]
        rows.append(row)

    # Create a DataFrame
    df = pd.DataFrame(rows, columns=[
        "Ár", "Launþegahópur", "Starfsstétt", "Kyn", 
        "Heildarlaun - fullvinnandi Meðaltal"
    ])

    # Save the DataFrame to a CSV file
    df.to_csv("data/wages_by_occupation_and_gender.csv", index=False)
    print("Data saved to data/wages_by_occupation_and_gender.csv")
    print(df)

# Fetch and save the wage distribution data
fetch_wage_distribution()
