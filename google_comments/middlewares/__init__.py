from google_comments.signals import Signal


class BaseMiddleware:
    def __call__(self, data, **kwargs):
        pass


class CompleteAddress(BaseMiddleware):
    def __call__(self, data, **kwargs):
        pass


complete_address_signal = Signal()
complete_address_signal.connect(CompleteAddress())
