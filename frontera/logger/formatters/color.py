from colorlog import ColoredFormatter
from colorlog.escape_codes import escape_codes


class ColorFormatter(ColoredFormatter):
    def __init__(
        self, format, log_colors, log_color_field, datefmt=None, reset=True, style="%"
    ):
        super().__init__(
            format, datefmt=datefmt, log_colors=log_colors, reset=reset, style=style
        )
        self.log_color_field = log_color_field

    def format(self, record):
        if not hasattr(record, self.log_color_field):
            setattr(record, self.log_color_field, "?")

        record.__dict__.update(escape_codes)

        color_field = self._get_color_field(record)
        if color_field and color_field in self.log_colors:
            color = self.log_colors[color_field]
            record.log_color = escape_codes[color]
        else:
            record.log_color = ""

        # Format the message
        message = super(ColoredFormatter, self).format(record)

        # Add a reset code to the end of the message
        # (if it wasn't explicitly added in format str)
        if self.reset and not message.endswith(escape_codes["reset"]):
            message += escape_codes["reset"]

        return message

    def _get_color_field(self, record):
        if not self.log_color_field:
            return None
        if hasattr(record, self.log_color_field):
            return getattr(record, self.log_color_field)
        if isinstance(record.msg, dict) and self.log_color_field in record.msg:
            return record.msg[self.log_color_field]
        return None
