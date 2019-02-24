LOG_LEVEL = 1


def log_debug(msg):
    if LOG_LEVEL > 0:
        print(str(msg))


def log(msg):
    print(str(msg))
