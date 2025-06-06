from abc import ABCMeta, abstractmethod


class BaseDecoder(metaclass=ABCMeta):
    @abstractmethod
    def decode(self, buffer):
        """
        Decodes the message.

        :param bytes buffer: encoded message
        :return: tuple of message type and related objects
        """

    @abstractmethod
    def decode_request(self, buffer):
        """
        Decodes Request objects.

        :param bytes buffer: serialized string
        :return: object Request
        """


class BaseEncoder(metaclass=ABCMeta):
    @abstractmethod
    def encode_add_seeds(self, seeds):
        """
        Encodes add_seeds message

        :param list seeds: A list of frontier Request objects
        :return: bytes encoded message
        """

    @abstractmethod
    def encode_page_crawled(self, response):
        """
        Encodes a page_crawled message

        :param object response: A frontier Response object

        :return: bytes encoded message
        """

    @abstractmethod
    def encode_links_extracted(self, request, links):
        """
        Encodes a links_extracted message

        :param object request: A frontier Request object
        :param list links: A list of Request objects

        :return: bytes encoded message
        """

    @abstractmethod
    def encode_request_error(self, request, error):
        """
        Encodes a request_error message

        :param object request: A frontier Request object
        :param string error: Error description

        :return: bytes encoded message
        """

    @abstractmethod
    def encode_request(self, request):
        """
        Encodes requests for spider feed stream.

        :param object request: Frontera Request object
        :return: bytes encoded message
        """

    @abstractmethod
    def encode_update_score(self, request, score, schedule):
        """
        Encodes update_score messages for scoring log stream.

        :param object request: Frontera Request object
        :param float score: score
        :param bool schedule: True if document needs to be scheduled for download
        :return: bytes encoded message
        """

    @abstractmethod
    def encode_new_job_id(self, job_id):
        """
        Encodes changing of job_id parameter.

        :param int job_id:
        :return: bytes encoded message
        """

    @abstractmethod
    def encode_offset(self, partition_id, offset):
        """
        Encodes current spider offset in spider feed.

        :param int partition_id:
        :param int offset:
        :return: bytes encoded message
        """
