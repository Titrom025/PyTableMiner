class TableData:
    def __init__(self):
        self.table = None
        self.markup = None

        self.row2object = {}
        self.row2predicate = {}
        self.row2date = {}

        self.col2object = {}
        self.col2predicate = {}
        self.col2date = {}

        self.target_tag = None
        self.sheet_name = ""