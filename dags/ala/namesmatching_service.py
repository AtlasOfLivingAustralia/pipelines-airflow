import requests
from requests.adapters import HTTPAdapter
from urllib.parse import urljoin
from urllib3.util import Retry
import concurrent.futures as cf
from enum import Enum
import pandas as pd
from pathlib import Path
import time
import logging

class Param(Enum):
    _HINTS     = "hints"
    _LOOSE     = "loose"
    KINGDOM    = "kingdom"
    PHYLUM     = "phylum"
    CLASS      = "clazz"
    ORDER      = "order"
    FAMILY     = "family"
    GENUS      = "genus"
    S_EPITHET  = "specificEpithet"
    I_EPITHET  = "infraspecificEpithet"
    RANK       = "rank"
    VERB_RANK  = "verbatimTaxonRank"
    AUTHORSHIP = "scientificNameAuthorship"
    SCI_NAME   = "scientificName"
    VERN_NAME  = "vernacularName"
    TAXON_ID   = "taxonID"

class RetParam(Enum):
    _IDX        = "idx"
    METHOD      = "method"
    PARAMS      = "params" 
    SUCCESS     = "success"
    ISSUES      = "issues"
    SCI_NAME    = "scientificName"
    AUTHORSHIP  = "scientificNameAuthorship"
    TAXON_ID    = "taxonConceptID"
    RANK        = "rank"
    RANK_ID     = "rankID"
    LEFT        = "lft"
    RIGHT       = "rgt"
    MATCH_TYPE  = "matchType"
    NAME_TYPE   = "nameType"
    KINGDOM     = "kingdom"
    KINGDOM_ID  = "kingdomID"
    PHYLUM      = "phylum"
    PHYLUM_ID   = "phylumID"
    CLASS       = "classs"
    CLASS_ID    = "classID"
    ORDER       = "order"
    ORDER_ID    = "orderID"
    FAMILY      = "family"
    FAMILY_ID   = "familyID"
    GENUS       = "genus"
    GENUS_ID    = "genusID"
    SPEC_GRP    = "speciesGroup"
    SPEC_SUBGRP = "speciesSubgroup"

class Env(Enum):
    TEST = "https://namematching-ws.test.ala.org.au"
    PROD = "https://namematching-ws-turbo.ala.org.au"
    STAGING = "https://namematching-staging-ws.ala.org.au"

class Method(Enum):
    POST = "POST"
    GET = "GET"

class NamesMatching:

    _exclude_prefix = (
        RetParam._IDX.value,
        RetParam.METHOD.value,
        RetParam.PARAMS.value,
        RetParam.SUCCESS.value,
        RetParam.ISSUES.value
    )

    _default_prefix = "returned"
    endpoint = "api/searchByClassification"

    def __init__(self, env: Env = Env.TEST, method: Method = Method.POST, max_workers: int = 10, prefix: str = ""):
        self.env = env
        self.method = method
        self.max_workers = max_workers
        self.prefix = prefix or self._default_prefix

        self.url = urljoin(env.value, self.endpoint)
        self.session: requests.Session = None

    def get_returned_name(self, param: RetParam) -> str:
        return self._apply_prefix(param.value)

    def get_prefixed_param_names(self) -> list[str]:
        return [self._prefix(item.value) for item in RetParam if item.value not in self._exclude_prefix]

    def _prefix(self, value: str) -> str:
        return f"{self.prefix}_{value}"

    def _apply_prefix(self, key: str) -> str:
        return self._prefix(key) if self.prefix and key not in self._exclude_prefix else key
        
    @staticmethod
    def start_session(func) -> callable:
        def wrapper(self, *args, **kwargs) -> any:
            if self.session is None:
                session = requests.Session()
                session.headers.update({"accept": "application/json", "User-Agent": "ala-names-matching-test/0.1"})
                retry_settings = Retry(total=5, backoff_factor=0.2)
                adapter = HTTPAdapter(pool_connections=self.max_workers, pool_maxsize=self.max_workers, max_retries=retry_settings)
                session.mount("https://", adapter)

                self.session = session

            return func(self, *args, **kwargs)
        return wrapper

    def _collect(self, params: dict, idx: int = -1) -> dict:
        # Set retrieve method info and index value
        ret_val = {
            RetParam._IDX.value: idx,
            RetParam.METHOD.value: self.method.value.lower(),
            RetParam.PARAMS.value: params
        }

        # Post parameters and check repsonse
        response = self.session.post(self.url, json=params) if self.method == Method.POST else self.session.get(self.url, params=params)
        if response.status_code != 200:
            return ret_val | {RetParam.SUCCESS.value: False, RetParam.ISSUES.value: [f"{response.status_code} ({response.reason}): {response.json()['message']}"]}

        # Decode json response
        try:
            data: dict = response.json()
        except requests.exceptions.JSONDecodeError:
            return ret_val | {RetParam.SUCCESS.value: False, RetParam.ISSUES.value: ["Issue decoding JSON"]}

        # Convert success to boolean values
        data[RetParam.SUCCESS.value] = data[RetParam.SUCCESS.value] == "TRUE"

        # Update noIssue for a failed match to noMatch
        if not data[RetParam.SUCCESS.value] and data[RetParam.ISSUES.value] == ["noIssue"]:
            data[RetParam.ISSUES.value] = ["noMatch"]

        # Prefix keys if required
        return ret_val | {self._apply_prefix(key): value for key, value in data.items()}
    
    @start_session
    def run_single(self, params: dict[str, str]) -> dict:
        ret_val = self._collect(params)
        ret_val.pop(RetParam._IDX.value)
        return ret_val

    @start_session
    def run_series(self, series: pd.Series) -> pd.DataFrame:
        records = []

        with cf.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = (executor.submit(self._collect, params, idx) for idx, params in series.items())

            for future in cf.as_completed(futures):
                records.append(future.result())

        return pd.DataFrame.from_records(records, index=RetParam._IDX.value).convert_dtypes()

    def run_df(self, df: pd.DataFrame) -> pd.DataFrame:
        valid_df_columns = df.columns.intersection([item.value for item in Param])
        param_series = df[valid_df_columns].apply(lambda row: {k: v for k, v in row.dropna().items() if v != ""}, axis=1)
        return self.run_series(param_series)

    def process_df(self, df: pd.DataFrame, mappings: dict[Param, str] = {}) -> pd.DataFrame:
        df = df.rename(columns={value: key.value for key, value in mappings.items()})
        return self.run_df(df).sort_index()

    def run_file(self, input_path: Path, output_path: Path, mappings: dict[Param, str] = {}, rows: int = 0, chunksize: int = 0) -> None:
        records_name = "all" if not rows else str(rows)
        read_kwargs = {
            "keep_default_na": False,
            "nrows": rows or None,
            "chunksize": chunksize or None,
            "iterator": chunksize == 0
        }

        logging.info(f"Running name matching in '{self.env.name.lower()}' on {records_name} records using {self.method.name} method with {self.max_workers} workers")

        total_timer = TimeKeeper()
        for idx, df in enumerate(pd.read_csv(input_path, **read_kwargs)):
            chunk_timer = TimeKeeper()
            df = self.process_df(df, mappings, f"[chunk {idx+1}]")

            # Reorder columns to align properly on subsequent writes
            if idx == 0:
                col_order = df.columns
            else:
                df = df[col_order]

            df.to_csv(output_path, mode="a", header=idx==0, index=False)
            logging.info(f"Chunk {idx+1} finished in: {chunk_timer.get_diff()} | Total time: {total_timer.get_diff()}")

        logging.info(f"Generated file {output_path}")

class TimeKeeper:
    def __init__(self):
        self.start_time = time.perf_counter_ns()

    def get_diff(self) -> str:
        total_time = (time.perf_counter_ns() - self.start_time) / 1000000000
        return f"{int(total_time // 3600):02}:{int(total_time // 60) % 60:02}:{total_time % 60:05.2f}"
