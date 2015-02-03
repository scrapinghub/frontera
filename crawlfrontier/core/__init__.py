class OverusedKeys(list):
    def __init__(self, type='domain'):
        super(OverusedKeys, self).__init__()
        self.type = type