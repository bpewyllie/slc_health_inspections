"""
Scraper functions for SL County Health Department inspections site.
"""

import io
import requests
import pandas

from bs4 import BeautifulSoup

import slc_health_inspections.constants.urls as urls


def scrape_pages(n_pages: int, get_detail: bool) -> list:
    """
    Scrapes SL County Food Service establishments and inspection details for a given number of
    pages.

    :param int n_pages: number of pages to scrape
    :param bool get_detail: whether to scrape details for each establishment

    :rtype: list
    :return: three pandas DataFrames: establishments, establishment details, and establishment
    inspections
    """

    url = urls.ESTABLISHMENTS_URL

    payload = {
        "ctl00$PageContent$CODE_DESCRIPTIONFilter": "Food Service",
    }

    page = 1

    with requests.Session() as s:

        s.headers["User-Agent"] = (
            "Mozilla/5.0 (WindowMozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36"
        )
        r = s.get(url)
        soup = BeautifulSoup(r.text, "lxml")
        payload["__VIEWSTATE"] = soup.select_one("#__VIEWSTATE")["value"]
        payload["__VIEWSTATEGENERATOR"] = soup.select_one("#__VIEWSTATEGENERATOR")[
            "value"
        ]
        est_df = pandas.DataFrame()
        agg_det_df1 = pandas.DataFrame()
        agg_det_df2 = pandas.DataFrame()

        while page <= n_pages:

            # Submit form and grab data
            print(f"Scraping page {page} of results...", end="\r")
            res = s.post(url, data=payload)
            soup = BeautifulSoup(res.text, "lxml")
            page_df = pandas.read_html(
                io.StringIO(soup.prettify()),
                attrs={"id": "VW_EST_PUBLICTableControlGrid"},
            )[0]
            page_df["Page"] = page
            est_df = pandas.concat(
                [est_df, page_df], ignore_index=True
            ).drop_duplicates()

            if get_detail:

                print(f"Scraping detail for page {page}...", end="\r")

                for e in range(len(est_df.loc[est_df["Page"] == page])):

                    # Click "Inspections" button for each establishment on page
                    idx = f"{e:02d}"
                    payload = {
                        i["name"]: i.get("value", "")
                        for i in soup.select("input[name]")
                    }
                    payload["__EVENTTARGET"] = (
                        f"ctl00$PageContent$VW_EST_PUBLICTableControlRepeater$ctl{idx}$InspButton$_Button"
                    )
                    payload["__EVENTARGUMENT"] = ""
                    res = s.post(url, data=payload)
                    det_soup = BeautifulSoup(res.text, "lxml")

                    # Scrape establishment detail, inspection history
                    det_df1 = (
                        pandas.read_html(io.StringIO(det_soup.prettify()))[8]
                        .set_index(0)
                        .transpose()
                    )
                    det_df1["idx"] = idx
                    det_df1["Page"] = page
                    agg_det_df1 = pandas.concat(
                        [agg_det_df1, det_df1], ignore_index=True
                    )

                    det_df2 = pandas.read_html(
                        io.StringIO(det_soup.prettify()),
                        attrs={"id": "INSPECTIONTableControlGrid"},
                    )[0]
                    det_df2["idx"] = idx
                    det_df2["Page"] = page
                    agg_det_df2 = pandas.concat(
                        [agg_det_df2, det_df2], ignore_index=True
                    )

                    # Go back to main table
                    back_url = urls.BACK_URL
                    back_payload = payload
                    back_payload = {
                        i["name"]: i.get("value", "")
                        for i in soup.select("input[name]")
                    }
                    back_payload["__VIEWSTATE"] = det_soup.select_one("#__VIEWSTATE")[
                        "value"
                    ]
                    back_payload["__VIEWSTATEGENERATOR"] = det_soup.select_one(
                        "#__VIEWSTATEGENERATOR"
                    )["value"]
                    back_payload["__EVENTTARGET"] = "ctl00$PageContent$OKButton$_Button"
                    back_payload["__EVENTARGUMENT"] = ""
                    res = s.post(back_url, data=back_payload)

            # Grab state variables for next page
            page += 1
            payload = {
                i["name"]: i.get("value", "") for i in soup.select("input[name]")
            }
            payload["__EVENTTARGET"] = (
                "ctl00$PageContent$VW_EST_PUBLICPagination$_NextPage"
            )
            payload["__EVENTARGUMENT"] = ""

            # Maybe not necessary - testing for loop
            soup = BeautifulSoup(res.text, "lxml")
            payload["__VIEWSTATE"] = soup.select_one("#__VIEWSTATE")["value"]
            payload["__VIEWSTATEGENERATOR"] = soup.select_one("#__VIEWSTATEGENERATOR")[
                "value"
            ]

    return est_df, agg_det_df1, agg_det_df2


if __name__ == "__main__":
    dfs = scrape_pages(54, True)
    dfs[0].to_csv("output/establishments.csv")
    dfs[1].to_csv("output/establishment_details.csv")
    dfs[2].to_csv("output/inspection_details.csv")
