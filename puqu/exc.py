class PuQuException(Exception):
    pass


class NotConnectedError(PuQuException):
    pass


class NotConfiguredError(PuQuException):
    pass


class StopListening(PuQuException):
    pass


class JobNameAlreadyRegistered(PuQuException):
    pass


class JobFunctionNotRegistered(PuQuException):
    pass
