# -*- coding: utf-8 -*-

import pandas as pd
import geopandas as gpd
import re

# %% General Setup

# folder contents are shared here https://drive.google.com/drive/folders/1_WzMgvKRXSy2_zR_IkEdV6EJ_-Ti2jaV?usp=sharing
main_fol = "D:/Stuff/Projects/2025_09_Strassenlisten/"
prep_fol = main_fol + "prep/"
out_fol = main_fol + "output/"

# read full NRW address list from openaddresses.io with appended PLZ
# extract from NRW__OA_inclPLZ.7z -> https://drive.google.com/file/d/1slbZRi0S7kmLXhrL9iyiUH_C4OufRNrS/view?usp=sharing
addr_fp = main_fol + "openaddresses/NRW__OA_inclPLZ.gpkg"
addr_gdf = gpd.read_file(addr_fp, engine="pyogrio", use_arrow=True)


# %% General Functions
def clean_streenames(df, street_col, out_col="strassen_name_clean"):
    df[out_col] = (
        df[street_col]
        .str.lower()
        .str.replace("ß", "ss")
        .str.replace(" strasse", "str")
        .str.replace("strasse", "str")
        .str.replace("str.", "str")
        .str.replace("platz", "pl")
        .str.replace("pl.", "pl")
        .str.replace(" ", "")
        .str.replace("-", "")
        .str.replace("ä", "ae")
        .str.replace("ö", "oe")
        .str.replace("ü", "ue")
        .copy()
    )
    return df


# %% Bad Berleburg
def assign_wahlbezirk_v2(
    street, number, gemeinde_df, bezirk_name="Stimmbezirk", range_column="HNr.-Bereich"
):
    """Use when Gemeinde provides a mapping of streets with a number span (eg 1-21, 5A-14)
    to Bezirk. Now supports house numbers with letters.

    Args:
        street: Street name to match
        number: House number (can include letters, e.g., "5A", "12b")
        gemeinde_df: DataFrame with street mappings and ranges
        bezirk_name: Column name for the Bezirk identifier
        range_column: Column name containing the house number ranges (e.g., "5A - 14")
    """

    def parse_house_number(house_num_str):
        """Parse house number into numeric and letter parts"""
        if pd.isna(house_num_str) or house_num_str == "":
            return None, None

        # Remove spaces and convert to string
        house_num_str = str(house_num_str).strip()

        # Extract numeric part and letter part
        match = re.match(r"^(\d+)([A-Za-z]?)$", house_num_str)
        if match:
            num_part = int(match.group(1))
            letter_part = match.group(2).upper() if match.group(2) else ""
            return num_part, letter_part
        else:
            return None, None

    def is_in_range(input_num, input_letter, range_str):
        """Check if input house number is within the given range"""
        if pd.isna(range_str) or range_str == "":
            return False

        # Split range string (handle different separators)
        range_str = str(range_str).strip()
        parts = re.split(r"\s*-\s*", range_str)
        if len(parts) != 2:
            return False

        start_str, end_str = parts[0].strip(), parts[1].strip()

        # Parse start and end numbers
        start_num, start_letter = parse_house_number(start_str)
        end_num, end_letter = parse_house_number(end_str)

        if start_num is None or end_num is None:
            return False

        # Check if input number is within range
        if input_num < start_num or input_num > end_num:
            return False

        # If input number equals start number, check letter
        if input_num == start_num:
            if start_letter == "" and input_letter == "":
                return True
            elif start_letter != "" and input_letter != "":
                return input_letter >= start_letter
            elif start_letter == "" and input_letter != "":
                return True  # 5A is >= 5
            else:  # start_letter != "" and input_letter == ""
                return False  # 5 is < 5A

        # If input number equals end number, check letter
        if input_num == end_num:
            if end_letter == "" and input_letter == "":
                return True
            elif end_letter != "" and input_letter != "":
                return input_letter <= end_letter
            elif end_letter == "" and input_letter != "":
                return False  # 14A is > 14
            else:  # end_letter != "" and input_letter == ""
                return True  # 14 is <= 14A

        # If between start and end numbers, always included
        return True

    # Parse input house number
    input_num, input_letter = parse_house_number(number)
    if input_num is None:
        print(f"Invalid house number format: {number}")
        return ""

    # Filter original_df for rows matching the given street
    filtered_df = gemeinde_df[gemeinde_df["strassen_name_clean"] == street]

    if len(filtered_df) == 0:
        print(f"No entries found for street {street}")
        return ""

    # Check each row to see if the house number falls within the range
    for idx, row in filtered_df.iterrows():
        range_str = row.get(range_column, "")
        if is_in_range(input_num, input_letter, range_str):
            return row[bezirk_name]

    # If no range matches, return empty string
    print(f"House number {number} on {street} not found in any range")
    return ""


badberleburg_addr_gdf = addr_gdf[addr_gdf["city"].str.contains("Bad Berleburg")].copy()
badberleburg_addr_gdf = clean_streenames(badberleburg_addr_gdf, "street")

badberleburg_gemeinde_df = pd.read_excel(
    main_fol
    + "prep/BadBerleburg_Bekanntmachung_des_Wahlleiters_der_Stadt_Bad_Berleburg_über_die_Eintei.xlsx"
)
# clean up data a bit
for col in ["Stimmbezirk", "HNr.-Bereich"]:
    badberleburg_gemeinde_df[col] = badberleburg_gemeinde_df[col].str.replace("'", "")

badberleburg_gemeinde_df = clean_streenames(badberleburg_gemeinde_df, "Straßenname")

# check example Stöppelsweg, with overlapping ranges in gemeinde data and letters in ranges
badberleburg_addr_gdf["BezirkNr"] = ""
badberleburg_addr_gdf["BezirkNr"] = badberleburg_addr_gdf.apply(
    lambda row: assign_wahlbezirk_v2(
        row["strassen_name_clean"],
        row["number"],
        badberleburg_gemeinde_df,
        range_column="HNr.-Bereich",
    ),
    axis=1,
)


badberleburg_addr_gdf.to_csv(
    out_fol + "badberleburg.csv", sep=";", encoding="utf8", index=False
)


# %% Hallenberg


def assign_wahlbezirk_v1(street, gemeinde_df, bezirk_name="Wahlbezirk"):
    """Use when Gemeinde provides a mapping of streets (no numbers) to Bezirk"""

    # Filter original_df for rows matching the given street
    filtered_df = gemeinde_df[gemeinde_df["strassen_name_clean"] == street]

    if len(filtered_df) == 0:
        print(f"No entries found for street {street}")
        return ""

    elif len(filtered_df) > 1:
        print(f"Warning: multiple entries for street {street} exist")
        print(filtered_df[["strassen_name_clean", bezirk_name]])

    return filtered_df.iloc[0][bezirk_name]


hallenberg_addr_gdf = addr_gdf[addr_gdf["city"].str.contains("Hallenberg")].copy()
hallenberg_addr_gdf = clean_streenames(hallenberg_addr_gdf, "street")

hallenberg_gemeinde_df = pd.read_excel(
    main_fol + "prep/Hallenberg_Anlage_1_-_Einteilung_Wahlgebiet.xlsx"
)
hallenberg_gemeinde_df = clean_streenames(hallenberg_gemeinde_df, "Straßenname")

hallenberg_addr_gdf["BezirkNr"] = ""
hallenberg_addr_gdf["BezirkNr"] = hallenberg_addr_gdf.apply(
    lambda row: assign_wahlbezirk_v1(row["strassen_name_clean"], hallenberg_gemeinde_df),
    axis=1,
)

hallenberg_addr_gdf.to_csv(out_fol + "Hallenberg.csv", sep=";", encoding="utf8", index=False)
