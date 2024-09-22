"""
Data analysis scripts for SL County Health Department inspection data.
"""

import os
import pandas

# from geopy.geocoders import Nominatim
import geopy


def get_location(address, geolocator):
    """
    Shortcut function for handling errors with geocoding.

    :param geopy.geocoders.Nominatim geolocator: geopy location
    :rtype: geopy.Location
    :return: geopy location
    """
    try:
        return geolocator.geocode(address, exactly_one=True, timeout=1)
    except:  # geopy.exc.GeocoderUnavailable:
        return None


def clean_data(
    establishments: pandas.DataFrame, inspections: pandas.DataFrame
) -> pandas.DataFrame:
    """
    Returns a cleaned dataset of all establishments and their inspections.

    :param pandas.DataFrame establishments: DataFrame of establishment details
    :param pandas.DataFrame inspections: DataFrame of inspection histories
    :rtype: pandas.DataFrame
    :return: cleaned DataFrame
    """

    # Geocode addresses to lat/lon pairs
    geolocator = geopy.geocoders.Nominatim(user_agent="slc_health_inspections")
    establishments["Full Address"] = (
        establishments["Address"] + "\n" + establishments["City/State/ZIP"]
    )
    establishments["Location"] = establishments["Full Address"].apply(
        lambda x: get_location(x, geolocator)
    )
    establishments.loc[establishments["Location"].notna(), "Latitude"] = (
        establishments.loc[establishments["Location"].notna(), "Location"].apply(
            lambda x: x.latitude
        )
    )
    establishments.loc[establishments["Location"].notna(), "Longitude"] = (
        establishments.loc[establishments["Location"].notna(), "Location"].apply(
            lambda x: x.longitude
        )
    )

    # Combine datasets and clean up
    df = pandas.merge(
        left=establishments, right=inspections, how="left", on=["idx", "Page"]
    )
    df["ID"] = df["Page"].astype(str) + "-" + df["idx"].astype(str)
    df.drop(
        columns=["Unnamed: 0_x", "Unnamed: 0.1", "Unnamed: 0_y", "Page", "idx"],
        inplace=True,
    )

    # Prep date field for regression
    df["Date_Int"] = pandas.to_datetime(df["Date"]).dt.strftime("%Y%m%d")
    df["Date_Int"] = pandas.to_numeric(df["Date_Int"], errors="coerce")
    df.loc[df["Date_Int"].notna(), "Date_Int"] = df.loc[
        df["Date_Int"].notna(), "Date_Int"
    ].astype(int)

    # Stats by Establishment
    df["Total Establishment Inspections"] = df.groupby("ID")["ID"].transform("count")
    df.sort_values(by=["ID", "Date_Int"], inplace=True)
    df["Inspection Number"] = df.groupby("ID")["Date_Int"].rank("dense")

    # Calculate least-squares time-series slope per ID
    cov_df = pandas.DataFrame(
        df.loc[df["Total Establishment Inspections"] > 1]
        .groupby("ID")[["Date_Int", "Score"]]
        .apply(lambda x: x["Date_Int"].cov(x["Score"]))
        .rename("Covariance")
    )
    df = pandas.merge(df, cov_df, how="left", on="ID")
    var_df = pandas.DataFrame(
        df.loc[df["Date_Int"].notna()]
        .groupby("ID")["Date_Int"]
        .var()
        .rename("Variance")
    )
    df = pandas.merge(df, var_df, how="left", on="ID")
    df["Slope"] = df["Covariance"] / df["Variance"]

    return df


if __name__ == "__main__":

    est_df = pandas.read_csv(
        os.path.join(os.getcwd(), "output", "establishment_details.csv")
    )
    ins_df = pandas.read_csv(
        os.path.join(os.getcwd(), "output", "inspection_details.csv")
    )

    all_data = clean_data(est_df, ins_df)
    # print(all_data.columns)
    # print(all_data.head())
    print(all_data.iloc[3])
    # print(all_data.describe())
    all_data.to_csv(os.path.join(os.getcwd(), "output", "clean", "clean_data.csv"))
