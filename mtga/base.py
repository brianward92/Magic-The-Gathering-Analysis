import csv
import gzip
import os
import pickle

import numpy as np
import pandas as pd
from scipy import sparse

from common.py import logs


log = logs.get_logger()

DATA_TYPES = [
    "draft",
    "game",
    "replay",
]

SETS = [
    "LTR",
]

LIMITED_TYPES = [
    "PremierDraft",
    "TradDraft",
    "Sealed",
    "TradSealed",
]


DEFAULT_DATA_DIR = "~/dat/17Lands"


class MTGReader(object):
    def __init__(self):
        raise NotImplementedError("Base class.")

    def setup_disk_meta(
        self,
        set_code,
        limited_type,
        data_type,
        dat_path,
    ):
        # input file
        dat_path = os.path.expanduser(dat_path)
        self.raw_file_path = os.path.join(
            dat_path, "raw", f"{data_type}_public.{set_code}.{limited_type}.csv.gz"
        )

        # cached usable format
        self.processed_dir = os.path.join(
            dat_path,
            "processed",
            f"data_type={data_type}::set_code={set_code}::limited_type={limited_type}",
        )
        os.makedirs(self.processed_dir, exist_ok=True)
        self._n_lines = None

        return

    @property
    def n_lines(self):
        if self._n_lines is None:
            with gzip.open(self.raw_file_path, "rt") as file:
                self._n_lines = sum(1 for line in file)
        return self._n_lines


## Game Data

CARD_POSITIONS = [
    "drawn",
    "deck",
    "opening_hand",
    "sideboard",
    "tutored",
]


class GameDataReader(MTGReader):

    def __init__(
        self,
        set_code,
        limited_type,
        dat_path=DEFAULT_DATA_DIR,
        chunk_size=10000,
    ):
        # set up input file, output cache directory
        self.setup_disk_meta(set_code, limited_type, "game_data", dat_path)

        # game data split b/w card and non-card data
        self.cached_noncard_data = os.path.join(self.processed_dir, "noncard_data.csv")
        self.cached_card_data = os.path.join(self.processed_dir, "card_data.pkl")

        # cache read
        self.noncard_data = None
        self.card_data = None
        self.is_loaded = False

        # metadata
        with gzip.open(self.raw_file_path, "rt") as file:
            header = next(csv.reader(file))
            self.set_card_meta(header)
            log.info(
                f"Created `GameDataReader` with the following non-card columns:\n"
                + ", ".join(self.noncard_columns)
            )
        self.chunk_size = chunk_size

        return

    def set_card_meta(self, header):
        # check
        assert len(set(header)) == len(header), "Duplicated columns!"

        # initialize meta
        self.card_meta = dict()  # positions -> cards
        self.noncard_columns = []  # fields not related to card position
        self.card_positions = []  # list of card positions
        self.card_names = []  # list of card names

        # loop over columns
        for column in header:
            is_card = False
            for card_position in CARD_POSITIONS:
                prefix = f"{card_position}_"
                if column.startswith(prefix):
                    is_card = True
                    break  # `card_position, prefix` needed later
            if is_card:
                card_name = column.replace(prefix, "")
                self.card_meta[card_position] = self.card_meta.get(
                    card_position, []
                ) + [card_name]
                self.card_positions.append(card_position)
                self.card_names.append(card_name)
            else:
                self.noncard_columns.append(column)

        # check column uniqueness
        card_positions = sorted(set(self.card_positions))
        card_names = set(self.card_names)
        for card_position in card_positions:
            s = set(self.card_meta[card_position])
            assert card_names == s
        return

    def cards_to_cards_sparse(self, cards):
        data = cards.values
        is_non_zero = data != 0
        data = data[is_non_zero]  # "auto-raveled"
        indptr, indices = np.where(is_non_zero)
        _, indptr = np.unique(indptr, return_counts=True)
        indptr = np.insert(indptr.cumsum(), 0, 0)
        return data, indices, indptr, cards.shape

    def get_data(self, force_refresh=False):
        if force_refresh:
            self.noncard_data = []
            self.card_data = []
            n_chunks = self.n_lines // self.chunk_size + 1
            for i, chunk in enumerate(
                pd.read_csv(self.raw_file_path, chunksize=self.chunk_size)
            ):
                self.noncard_data.append(chunk[self.noncard_columns])
                chunk = chunk.drop(self.noncard_columns, axis=1)
                data, indptr, cols, shape = self.cards_to_cards_sparse(chunk)
                arr = sparse.csr_matrix((data, indptr, cols), shape)
                assert (arr == chunk.values).all()
                self.card_data.append(arr)
                log.info(f"Processed chunk {i+1}/{n_chunks}.")
            self.noncard_data = pd.concat(self.noncard_data)
            self.card_data = sparse.vstack(self.card_data)
            self.card_data = self.card_data.tocsc()
            self.noncard_data.to_csv(self.cached_noncard_data, index=False)
            log.info(f"Wrote non-card data to {self.cached_noncard_data}.")
            with open(self.cached_card_data, "wb") as file:
                pickle.dump(self.card_data, file)
            log.info(f"Wrote card data to {self.cached_card_data}.")
        elif self.is_loaded:
            return self.noncard_data, self.card_data
        else:
            is_written = os.path.exists(self.cached_noncard_data) and os.path.exists(
                self.cached_card_data
            )
            if is_written:
                self.noncard_data = pd.read_csv(self.cached_noncard_data)
                with open(self.cached_card_data, "rb") as file:
                    self.card_data = pickle.load(file)
            else:  # user does not know what they are doing...
                return self.get_data(force_refresh=True)
        res = {"noncard_data": self.noncard_data, "card_data": self.card_data}
        return res


## Replay Data

CATEGORY_TO_PATTERN = {
    "deck": r"^deck_(.+)",
    "sideboard": r"^sideboard_(.+)",
    "turn": r"^(.*)_turn_(\d+)_(.*)$",
}


class ReplayDataReader(MTGReader):
    def __init__(
        self,
        set_code,
        limited_type,
        dat_path=DEFAULT_DATA_DIR,
        chunk_size=10000,
    ):
        # set up input file, output cache directory
        self.setup_disk_meta(set_code, limited_type, "replay_data", dat_path)
