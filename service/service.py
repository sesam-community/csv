import os
import json
import logger
import csv
from flask import Flask, request, Response
import requests
import pandas as pd
import re
from io import StringIO, BytesIO

app = Flask(__name__)

logger = logger.Logger("csv", os.environ.get("LOGLEVEL", "INFO"))

TO_CSV_DEFAULTS = {
    "csv_sep": ",",
    "csv_na_rep": "",
    "csv_float_format": None,
    "csv_columns": "",
    "csv_header": "True",
    "csv_index": "False",
    "csv_index_label": "_id",
    "csv_encoding": "utf-8",
    "csv_line_terminator": "\n",
    "csv_quoting": "MINIMAL",
    "csv_quotechar": "\"",
    "csv_doublequote": "True",
    "csv_escapechar": None,
    "csv_date_format": None,
    "csv_decimal": "."
}

READ_CSV_DEFAULTS = {
    "csv_sep": ",",
    "csv_delimiter": None,
    "csv_delim_whitespace": "False",
    "csv_header": "infer",
    "csv_names": None,
    "csv_usecols": None,
    "csv_prefix": None,
    "csv_mangle_dupe_cols": "True",
    "csv_true_values": None,
    "csv_false_values": None,
    "csv_skipinitialspace": "False",
    "csv_skiprows": None,
    "csv_skipfooter": "0",
    "csv_nrows": None,
    "csv_thousands": None,
    "csv_decimal": ".",
    "csv_float_precision": None,
    "csv_lineterminator": None,
    "csv_quotechar": "\"",
    "csv_quoting": "MINIMAL",
    "csv_doublequote": "True",
    "csv_escapechar": None,
    "csv_comment": None,
    "csv_encoding": "utf-8",
    "csv_dialect": None,
    "csv_error_bad_lines": "True",
    "csv_warn_bad_lines": "True",
    "csv_low_memory": "True"
}

QUOTING_OPTIONS = {
    "ALL": csv.QUOTE_ALL,
    "MINIMAL": csv.QUOTE_MINIMAL,
    "NONNUMERIC": csv.QUOTE_NONNUMERIC,
    "NONE": csv.QUOTE_NONE
}

TO_JSON_DEFAULTS = {
    "json_orient": "records",
    "json_date_format": "iso",
    "json_double_precision": "10",
    "json_force_ascii": "True",
    "json_date_unit": "ms",
    "json_lines": "False",
    "json_index": "True"
}

SESAM_FIELDS = [
    "_id",
    "_deleted",
    "_updated",
    "_hash",
    "_previous",
    "_ts",
    "_tracked"
]


def json_to_csv(dict, args):

    csv_sep = args.get("csv_sep", TO_CSV_DEFAULTS.get("csv_sep"))
    csv_na_rep = args.get("csv_na_rep", TO_CSV_DEFAULTS.get("csv_na_rep"))
    csv_float_format = args.get("csv_float_format",
                                TO_CSV_DEFAULTS.get("csv_float_format"))
    sesam_fields_wl = args.get("sesam_fields_wl", "").split(",")
    if sesam_fields_wl == [""]:
        sesam_fields_wl = []
    csv_columns = args.get("csv_columns",
                           TO_CSV_DEFAULTS.get("csv_columns")).split(",")
    if csv_columns == [""]:
        csv_columns = None
    csv_header = args.get("csv_header",
                          TO_CSV_DEFAULTS.get("csv_header")).split(",")
    if len(csv_header) == 1:
        csv_header = csv_header[0].lower() == "true"
    csv_index = args.get("csv_index",
                         TO_CSV_DEFAULTS.get("csv_index")).lower() == "true"

    csv_encoding = args.get("csv_encoding",
                            TO_CSV_DEFAULTS.get("csv_encoding"))
    csv_line_terminator = args.get("csv_line_terminator",
                                   TO_CSV_DEFAULTS.get("csv_line_terminator"))
    csv_quoting = QUOTING_OPTIONS.get(
        args.get("csv_quoting",
                 TO_CSV_DEFAULTS.get("csv_quoting")))
    csv_quotechar = args.get("csv_quotechar",
                             TO_CSV_DEFAULTS.get("csv_quotechar"))
    csv_doublequote = args.get(
        "csv_doublequote",
        TO_CSV_DEFAULTS.get("csv_doublequote")).lower() == "true"
    csv_escapechar = args.get("csv_escapechar",
                              TO_CSV_DEFAULTS.get("csv_escapechar"))
    csv_date_format = args.get("csv_date_format",
                               TO_CSV_DEFAULTS.get("csv_date_format"))
    csv_decimal = args.get("csv_decimal", TO_CSV_DEFAULTS.get("csv_decimal"))

    df = pd.DataFrame(dict)
    logger.debug("Dropping columns %s",
                 set(SESAM_FIELDS) - set(sesam_fields_wl))
    df = df.drop(
        columns=list(set(SESAM_FIELDS) - set(sesam_fields_wl)),
        errors='ignore')
    if args.get("transit_decode", "").lower() == "true":
        df = df.replace("^~([rtbuf]|\:.*?\:)", "", regex=True)

    return df.to_csv(
        path_or_buf=None,
        sep=csv_sep,
        na_rep=csv_na_rep,
        float_format=csv_float_format,
        columns=csv_columns,
        header=csv_header,
        index=csv_index,
        index_label=None,
        mode='w',
        encoding=csv_encoding,
        compression=None,
        quoting=csv_quoting,
        quotechar=csv_quotechar,
        line_terminator=csv_line_terminator,
        chunksize=None,
        tupleize_cols=None,
        date_format=csv_date_format,
        doublequote=csv_doublequote,
        escapechar=csv_escapechar,
        decimal=csv_decimal), df.shape


def csv_to_json(url_or_buffer, args):

    csv_sep = args.get("csv_sep", READ_CSV_DEFAULTS.get("csv_sep"))
    csv_delimiter = args.get("csv_delimiter",
                             READ_CSV_DEFAULTS.get("csv_delimiter"))
    csv_delim_whitespace = args.get(
        "csv_delim_whitespace",
        READ_CSV_DEFAULTS.get("csv_delim_whitespace")).lower() == "true"
    csv_header = args.get("csv_header", READ_CSV_DEFAULTS.get("csv_header"))
    if re.match(r"^\d+(,\d)*", csv_header):
        csv_header = list(map(int, csv_header.split(",")))
    csv_names = args.get("csv_names", READ_CSV_DEFAULTS.get("csv_names"))
    if csv_names:
        csv_names = csv_names.split(",")
    csv_index_col = args.get("csv_index_col",
                             READ_CSV_DEFAULTS.get("csv_index_col"))
    if csv_index_col:
        if re.match(r"^\d+(,\d)*$", csv_index_col):
            csv_index_col = list(map(int, csv_index_col.split(",")))
        elif csv_index_col.lower() in ["true", "false"]:
            csv_index_col = csv_index_col.lower() == "true"
        else:
            csv_index_col = csv_index_col.split(",")
    csv_usecols = args.get("csv_usecols", READ_CSV_DEFAULTS.get("csv_usecols"))
    if csv_usecols:
        if re.match(r"^\d+(,\d)*", csv_usecols):
            csv_usecols = list(map(int, csv_usecols.split(",")))
        else:
            csv_usecols = csv_usecols.split(",")
    csv_prefix = args.get("csv_prefix", READ_CSV_DEFAULTS.get("csv_prefix"))
    csv_mangle_dupe_cols = args.get(
        "csv_mangle_dupe_cols",
        READ_CSV_DEFAULTS.get("csv_mangle_dupe_cols")).lower() == "true"
    csv_true_values = args.get("csv_true_values",
                               READ_CSV_DEFAULTS.get("csv_true_values"))
    if csv_true_values:
        csv_true_values = csv_true_values.split(",")
    csv_false_values = args.get("csv_false_values",
                                READ_CSV_DEFAULTS.get("csv_false_values"))
    if csv_false_values:
        csv_false_values = csv_false_values.split(",")
    csv_skipinitialspace = args.get(
        "csv_skipinitialspace",
        READ_CSV_DEFAULTS.get("csv_skipinitialspace")).lower() == "true"
    csv_skiprows = args.get("csv_skiprows",
                            READ_CSV_DEFAULTS.get("csv_skiprows"))
    if csv_skiprows:
        if re.match(r"^\d+(,\d)*", csv_skiprows):
            csv_skiprows = list(map(int, csv_skiprows.split(",")))
    csv_skipfooter = int(
        args.get("csv_skipfooter",
                 READ_CSV_DEFAULTS.get("csv_skipfooter")))
    csv_nrows = args.get("csv_nrows", READ_CSV_DEFAULTS.get("csv_nrows"))
    csv_thousands = args.get("csv_thousands",
                             READ_CSV_DEFAULTS.get("csv_thousands"))
    csv_decimal = args.get("csv_decimal", READ_CSV_DEFAULTS.get("csv_decimal"))
    csv_float_precision = args.get(
        "csv_float_precision",
        READ_CSV_DEFAULTS.get("csv_float_precision"))
    csv_lineterminator = args.get("csv_lineterminator",
                                  READ_CSV_DEFAULTS.get("csv_lineterminator"))
    csv_quotechar = args.get("csv_quotechar",
                             READ_CSV_DEFAULTS.get("csv_quotechar"))
    csv_quoting = QUOTING_OPTIONS.get(
        args.get("csv_quoting",
                 READ_CSV_DEFAULTS.get("csv_quoting")).upper())
    csv_doublequote = args.get(
        "csv_doublequote",
        READ_CSV_DEFAULTS.get("csv_doublequote")).lower() == "true"
    csv_escapechar = args.get("csv_escapechar",
                              READ_CSV_DEFAULTS.get("csv_escapechar"))
    csv_comment = args.get("csv_comment", READ_CSV_DEFAULTS.get("csv_comment"))
    csv_encoding = args.get("csv_encoding",
                            READ_CSV_DEFAULTS.get("csv_encoding"))
    csv_dialect = args.get("csv_dialect", READ_CSV_DEFAULTS.get("csv_dialect"))
    csv_error_bad_lines = args.get(
        "csv_error_bad_lines",
        READ_CSV_DEFAULTS.get("csv_error_bad_lines")).lower() == "true"
    csv_warn_bad_lines = args.get(
        "csv_warn_bad_lines",
        READ_CSV_DEFAULTS.get("csv_warn_bad_lines")).lower() == "true"
    csv_low_memory = args.get(
        "csv_low_memory",
        READ_CSV_DEFAULTS.get("csv_low_memory")).lower() == "true"

    json_orient = args.get("json_orient", TO_JSON_DEFAULTS.get("json_orient"))
    json_date_format = args.get("json_date_format",
                                TO_JSON_DEFAULTS.get("json_date_format"))
    json_double_precision = int(
        args.get("json_double_precision",
                 TO_JSON_DEFAULTS.get("json_double_precision")))
    json_force_ascii = args.get(
        "json_force_ascii",
        TO_JSON_DEFAULTS.get("json_force_ascii")).lower() == "true"
    json_date_unit = args.get("json_date_unit",
                              TO_JSON_DEFAULTS.get("json_date_unit"))
    json_lines = args.get("json_lines",
                          TO_JSON_DEFAULTS.get("json_lines")).lower() == "true"
    json_index = args.get("json_index",
                          TO_JSON_DEFAULTS.get("json_index")).lower() == "true"

    df = pd.read_csv(
        filepath_or_buffer=url_or_buffer,
        sep=csv_sep,
        delimiter=csv_delimiter,
        header=csv_header,
        names=csv_names,
        index_col=csv_index_col,
        usecols=csv_usecols,
        squeeze=False,
        prefix=csv_prefix,
        mangle_dupe_cols=csv_mangle_dupe_cols,
        dtype=None,
        engine=None,
        converters=None,
        true_values=csv_true_values,
        false_values=csv_false_values,
        skipinitialspace=csv_skipinitialspace,
        skiprows=csv_skiprows,
        nrows=csv_nrows,
        na_values=None,
        keep_default_na=True,
        na_filter=True,
        verbose=False,
        skip_blank_lines=True,
        parse_dates=False,
        infer_datetime_format=False,
        keep_date_col=False,
        date_parser=None,
        dayfirst=False,
        iterator=False,
        chunksize=None,
        compression="infer",
        thousands=csv_thousands,
        decimal=csv_decimal,
        lineterminator=csv_lineterminator,
        quotechar=csv_quotechar,
        quoting=csv_quoting,
        escapechar=csv_escapechar,
        comment=csv_comment,
        encoding=csv_encoding,
        dialect=csv_dialect,
        tupleize_cols=None,
        error_bad_lines=csv_error_bad_lines,
        warn_bad_lines=csv_warn_bad_lines,
        skipfooter=csv_skipfooter,
        doublequote=csv_doublequote,
        delim_whitespace=csv_delim_whitespace,
        low_memory=csv_low_memory,
        memory_map=False,
        float_precision=csv_float_precision)
    return df.to_json(
        orient=json_orient,
        date_format=json_date_format,
        double_precision=json_double_precision,
        force_ascii=json_force_ascii,
        date_unit=json_date_unit,
        default_handler=None,
        lines=json_lines,
        compression=None,
        index=json_index), df.shape


def service_response(response_code, message):
    return Response(
        response=json.dumps({
            "is_success": (response_code == 200),
            "message": message
        }),
        status=response_code,
        content_type="application/json")


@app.route("/upload", methods=["POST"])
def post():
    try:
        url = request.args.get("url")
        if not url:
            return service_response(400, "missing mandatory variable")
        csv_data, shape = json_to_csv(request.get_json(), request.args)
        logger.debug(
            "POSTing (rows, columns)=%s amount of data to %s" % (shape,
                                                                 url))
        r = requests.post(
            url,
            data=csv_data,
            headers={"Content-Type": "text/csv"})

        return service_response(r.status_code, r.text)
    except Exception as e:
        logger.exception(e)
        return service_response(500, str(e))


@app.route("/download", methods=["GET"])
def get():
    try:
        url = request.args.get("url")
        if not url:
            return service_response(400, "missing mandatory variable")
        json_data, shape = csv_to_json(url, request.args)
        logger.debug(
            "Fetched (rows, columns)=%s amount of data from  %s" % (shape,
                                                                    url))

        return Response(
            response=json_data,
            status=200,
            headers={"Content-Type": "application/json"})

    except Exception as e:
        logger.exception(e)
        return service_response(500, str(e))


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
