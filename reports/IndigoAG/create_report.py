"""
Extracts and transforms data to match requested csv output file 
format for an automated weekly report for Indigo_AG.
Purpose: Indigo_AG wants participation and score data for their org segments.

This module has been slightly modified from its original version
created by Symphony team. The original existed in a .ipynb file.

Original code did not contain comments. Please read through carefully.

Part II creates additional requested files added in July 2019.

Author: Eric Lee
"""
import json
import numpy as np
import os
import pandas as pd
import time

# this import contains the sql queries as class variables
from sql import Queries
from DA_package import mysql

start = time.time()
# load response rates (RR) and wins count for each active org
# with open("config.json", "r") as f:
#     config = json.load(f)

org_id = 32995  # Indigo AG
Q = Queries()

# TINYPULSE_REPLICA priveleges no longer required for this query to run
def mod_query(query, var_val, var="@organization_id"):
    """
    Modifies the query by changing the varia    ble (var) for required data.

    Params:
    -------
    query (str): MySQL query
    var_val (int/str): variable value(s)
    db_config (str): database login in configuration
    var (str): "@--" variable in sql query
    """
    sql_query = query.replace(var, str(var_val))
    return mysql.sql_query_to_pd(sql_query)


def convert_question_type(type):
    if type == "Question::ScaleQuestion":
        return "Scale"
    if type == "Question::BooleanQuestion":
        return "Yes/No"
    if type == "Question::TextQuestion":
        return "Text"
    return None


def main(save_file_path="~/airflow/files/"):
    # create dataframes from queries
    survey_df = mod_query(Q.survey, org_id)
    response_df = mod_query(Q.response, org_id)
    segment_df = mod_query(Q.segment, org_id)
    filter_df = mod_query(Q.filter, org_id)

    # used to pull filter labels from 'filterships' table. see sql query if confused.
    str_filter_ids = ",".join(str(x) for x in filter_df["filter_id"].values)
    filter_apply_df = mod_query(
        Q.filter_apply, var_val=str_filter_ids, var="@filter_ids"
    )

    # combine into one dataframe
    response_filter_df = filter_apply_df.merge(filter_df, on=["filter_id"], how="left")
    response_segment_df = response_df.merge(segment_df, on=["segment_id"], how="left")
    response_filter_segment_df = response_segment_df.merge(
        response_filter_df, on=["response_id"]
    )
    response_all_df = response_filter_segment_df.merge(
        survey_df, on=["survey_id"], how="left"
    )

    # group by and aggregations for output dataframe
    final_df = (
        response_all_df.groupby(
            [
                "survey_id",
                "prompt",
                "survey_day",
                "type",
                "filter_id",
                "filter_label",
                "segment_id",
                "segment_label",
            ]
        )
        .agg(
            {
                "response_id": pd.Series.nunique,
                "question_response_integer": np.sum,
                "submitted": np.sum,
            }
        )
        .reset_index()
    )

    org_survey_df = (
        response_df.groupby(["survey_id"])
        .agg(
            {
                "response_id": pd.Series.nunique,
                "question_response_integer": np.sum,
                "submitted": np.sum,
            }
        )
        .reset_index()
    )

    org_survey_df["org_participation_rate"] = (
        org_survey_df["submitted"] / org_survey_df["response_id"]
    )

    deliver_df = final_df.merge(
        org_survey_df[["survey_id", "org_participation_rate"]], on=["survey_id"]
    )

    deliver_df["avg_score"] = (
        deliver_df["question_response_integer"] / deliver_df["submitted"]
    )
    deliver_df["participation_rate"] = (
        deliver_df["submitted"] / deliver_df["response_id"]
    )

    deliver_df["avg_score"] = deliver_df[["type", "avg_score"]].apply(
        lambda x: x["avg_score"] if x["type"] == "Question::ScaleQuestion" else None,
        axis=1,
    )
    deliver_df["type"] = deliver_df["type"].apply(convert_question_type)

    # create average score column by (filter & segment)
    deliver_df2 = deliver_df.copy()
    segments = deliver_df2.segment_label.unique()
    deliver_df2 = deliver_df2.loc[deliver_df2["type"] == "Scale"].copy()
    deliver_df["avg_score_segment&filter"] = np.nan
    for segment in segments:
        s_filters = (
            deliver_df2["filter_label"]
            .loc[deliver_df2["segment_label"] == segment]
            .unique()
        )
        for s_filter in s_filters:
            index_tmp = deliver_df2.loc[
                (deliver_df2["segment_label"] == segment)
                & (deliver_df2["filter_label"] == s_filter)
            ].index
            deliver_df.loc[index_tmp, "avg_score_segment&filter"] = deliver_df2.loc[
                index_tmp, "avg_score"
            ].mean()

    columns = [
        "prompt",
        "survey_day",
        "filter_label",
        "segment_label",
        "avg_score_segment&filter",
        "participation_rate",
        "org_participation_rate",
    ]

    header = [
        "Question",
        "Date Asked",
        "Filter",
        "Segment",
        "Avg Score by Segment&Filter",
        "Participation rate",
        "Org Avg Participation Rate",
    ]

    deliver_df.to_csv(
        save_file_path + "Indigo_report.csv",
        index=None,
        header=header,
        columns=columns,
        encoding="utf-8",
    )

    # ***************************** PART II (UPDATED) *********************************
    # # Score Breakdown
    # Avg Score breakdown: # of each rating/% of each rating
    cols = [
        "question_response_integer",
        "submitted",
        "survey_id",
        "segment_id",
        "type",
        "survey_day",
    ]
    score_df = response_df.merge(
        survey_df, left_on="survey_id", right_on="survey_id", how="left"
    )[cols]

    score_df["type"] = score_df["type"].apply(convert_question_type)
    # split between boolean and scale
    boolean_df = score_df.loc[score_df["type"] == "Yes/No"].copy()
    scale_df = score_df.loc[score_df["type"] == "Scale"].copy()

    def create_score_df(df, vals):
        _dict = {}
        pct_dict = {}
        survey_ids = df["survey_id"].unique()
        for s_id in survey_ids:
            df_tmp = df.loc[df["survey_id"] == s_id].copy()
            # count num of each value
            survey_vals = [df_tmp["survey_day"].values[0]]
            for val in vals:
                survey_vals.append(
                    df_tmp.loc[df_tmp["question_response_integer"] == val].shape[0]
                )
            survey_vals.append(df_tmp["question_response_integer"].isnull().sum())
            _dict[s_id] = survey_vals

            pct_survey_vals = survey_vals.copy()
            pct_survey_vals[1:] = np.array(pct_survey_vals[1:]) / df_tmp.shape[0]
            pct_dict[s_id] = pct_survey_vals

        vals.append("nan")
        vals.insert(0, "date")
        score_df = pd.DataFrame().from_dict(_dict, orient="index", columns=vals)
        pct_df = pd.DataFrame().from_dict(pct_dict, orient="index", columns=vals)
        return score_df, pct_df

    boolean_vals = [0, 1]
    boolean_score_df, boolean_pct_df = create_score_df(boolean_df, boolean_vals)

    scale_vals = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    scale_score_df, scale_pct_df = create_score_df(scale_df, scale_vals)

    boolean_score_df.to_csv(save_file_path + "boolean_score.csv", index=None)
    boolean_pct_df.to_csv(save_file_path + "boolean_pct.csv", index=None)
    scale_score_df.to_csv(save_file_path + "scale_score.csv", index=None)
    scale_pct_df.to_csv(save_file_path + "scale_pct.csv", index=None)


if __name__ == "__main__":
    main()
