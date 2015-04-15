
from abc import ABCMeta, abstractmethod


class BaseHandler(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def handle(self, document):
        pass
