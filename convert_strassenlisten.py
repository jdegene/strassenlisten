# -*- coding: utf-8 -*-

import pandas as pd
import geopandas as gpd

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
