# -*- coding: utf-8 -*-

import pandas as pd
import geopandas as gpd

# %% General Setup

main_fol = "D:/Stuff/Projects/2025_09_Strassenlisten/"

# read full NRW address list from openaddresses.io
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


# %% Hans
