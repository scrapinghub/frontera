class NotConfigured(Exception):
    """Indicates a missing configuration situation"""
    def __init__(self, msg=None):
        self.msg = msg or ''

    def __str__(self):
        return self.msg
