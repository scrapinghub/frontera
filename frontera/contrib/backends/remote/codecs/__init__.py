# -*- coding: utf-8 -*-
from abc import abstractmethod, ABCMeta


class BaseDecoder(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def decode(self, buffer):
        """
        Decodes the message

        :param bytes message encoded message
        :return tuple of message type and related objects
        """
        pass

    @abstractmethod
    def decode_request(self, buffer):
        pass


class BaseEncoder(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def encode_add_seeds(self, seeds):
        """
        Encodes add_seeds message

        :param list seeds A list of frontier Request objects
        :return bytes encoded message
        """
        pass

    @abstractmethod
    def encode_page_crawled(self, response, links):
        """
        Encodes a page_crawled message

        :param object response A frontier Response object
        :param list links A list of Request objects

        :return bytes encoded message
        """
        pass

    @abstractmethod
    def encode_request_error(self, request, error):
        """
        Encodes a request_error message
        :param object request A frontier Request object
        :param string error Error description

        :return bytes encoded message
        """
        pass

    @abstractmethod
    def encode_request(self, request):
        pass

    @abstractmethod
    def encode_update_score(self, fingerprint, score, url, schedule):
        pass

    @abstractmethod
    def encode_new_job_id(self, job_id):
        pass

    @abstractmethod
    def encode_offset(self, partition_id, offset):
        pass