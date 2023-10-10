import pandas as pd


CARD_POSITIONS = ["opening_hand", "drawn", "deck", "sideboard"]

class MTGReader(object):

    def __init__(self, set_code, limited_type, data_type, dat_path="~/dat/17Lands"):
        self.input_data_path = f"{dat_path}/{data_type}_public.{set_code}.{limited_type}.csv"
        self.data = pd.read_csv(self.input_data_path)
        # TODO: auto-untar
        if data_type == "game_data":
            self._set_game_data_meta()
        return

    def _set_game_data_meta(self):
        # a check
        assert len(set(self.data.columns)) == self.data.shape[1], "Duplicated columns!"
        self.card_meta = dict()
        self.noncard_columns = []
        for column in self.data.columns:
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
                assert self.card_meta[CARD_POSITIONS[i]] == self.card_meta[CARD_POSITIONS[j]]
        return
