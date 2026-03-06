import pandas as pd
import json

def csv_to_json(csv_file, json_file=None, orient="records"):
    """
    Convert a CSV file to JSON.

    Parameters
    ----------
    csv_file : str
        Path to input CSV file.
    json_file : str or None
        Optional path to output JSON file. If None, JSON is returned.
    orient : str
        JSON structure (default 'records').

    Returns
    -------
    str or None
        JSON string if json_file is None, otherwise writes to file.
    """

    df = pd.read_csv(csv_file)

    if json_file:
        df.to_json(json_file, orient=orient, indent=2)
        return None
    else:
        return df.to_json(orient=orient, indent=2)




csv_to_json("hd2024.csv", "ipeds_schools.json")


