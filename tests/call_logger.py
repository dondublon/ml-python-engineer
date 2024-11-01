

class MethodCallLogger:
    def __init__(self, method):
        self.method = method
        self.call_args_list = []

    def __call__(self, *args, **kwargs):
        self.call_args_list.append((args, kwargs))
        return self.method(*args, **kwargs)