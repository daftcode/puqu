class PuquException(Exception):
    pass


class NotConnectedError(PuquException):
    pass


class NotConfiguredError(PuquException):
    pass


class StopListening(PuquException):
    pass


class JobNameAlreadyRegistered(PuquException):
    pass
