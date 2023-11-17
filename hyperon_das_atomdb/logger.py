import logging

LOG_FILE_NAME = '/tmp/das.log'
LOGGING_LEVEL = logging.DEBUG


class Logger:
    __instance = None

    @staticmethod
    def get_instance():
        if Logger.__instance is None:
            return Logger()
        return Logger.__instance

    def __init__(self):
        if Logger.__instance is not None:
            raise Exception("Invalid re-instantiation of Logger")
        else:
            print(
                "Log initialized <Hyperon DAS AtomDB>. "
                f"Log file: {LOG_FILE_NAME}"
            )
            logging.basicConfig(
                filename=LOG_FILE_NAME,
                level=LOGGING_LEVEL,
                format='%(asctime)s %(levelname)-8s %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
            )
            Logger.__instance = self

    def _prefix(self):
        return ""

    def debug(self, msg):
        logging.debug(self._prefix() + msg)

    def info(self, msg):
        logging.info(self._prefix() + msg)

    def warning(self, msg):
        logging.warning(self._prefix() + msg)

    def error(self, msg):
        logging.error(self._prefix() + msg)


def logger():
    return Logger.get_instance()
