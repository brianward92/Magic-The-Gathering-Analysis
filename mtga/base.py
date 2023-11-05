import csv
import gzip
import os


DATA_TYPES = [
    'draft',
    'game',
    'replay',
]

SETS = [
    'LTR',
]

LIMITED_TYPES = [
    'PremierDraft',
    'TradDraft',
    'Sealed',
    'TradSealed',
]

CARD_POSITIONS = [
    "drawn",
    "deck",
    "opening_hand",
    "sideboard",
    "tutored",
]

class MTGReader(object):

    def __init__(self, set_code, limited_type, data_type, dat_path="~/dat/17Lands"):
        self.file_path = f'{dat_path}/{data_type}_public.{set_code}.{limited_type}.csv.gz'
        self.file_path = os.path.expanduser(self.file_path)
        with gzip.open(file_path, 'rt') as file:
            header = next(csv.reader(file))
            self.set_card_meta(header)
        return

    def file_meta(self, df=None):
        if (self.file_meta is None) and (df is None):
            print('WARNING - file_meta not set, ')
            file_meta
        return file_meta

    def set_card_meta(self, header):
        assert len(set(header)) == len(header), "Duplicated columns!"
        self.card_meta = dict()
        self.noncard_columns = []
        for column in header:
            is_card = False
            for card_position in CARD_POSITIONS:
                prefix = f"{card_position}_"
                if column.startswith(prefix):
                    is_card = True
                    break # `card_position, prefix` needed later
            if is_card:
                card = column.replace(prefix, "")
                self.card_meta[card_position] = self.card_meta.get(card_position, []) + [card]
            else:
                self.noncard_columns.append(column)
        self.card_meta = {card_position:sorted(cards) for card_position, cards in
                          self.card_meta.items()}
        for i in range(len(CARD_POSITIONS)):
            for j in range(i+1, len(CARD_POSITIONS)):
                assert card_meta[CARD_POSITIONS[i]] == card_meta[CARD_POSITIONS[j]]
        return
