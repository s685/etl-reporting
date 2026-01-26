class ClaimStatusException(Exception):
    """
    Custom exception for claim status mapping errors.
    """

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message
