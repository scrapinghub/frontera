class NotConfigured(Exception):
    """Indicates a missing configuration situation"""
    pass

class MissingRequiredField(Exception):
    """Indicates a required field is missing"""
    pass
