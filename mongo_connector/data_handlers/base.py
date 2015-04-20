
"""External handler functions which are meant to change documents
fetched from mongo need to have at least @data_handler decorator applied.
Without that, they won't be called.
If you have functions which might raise an exception, or otherwise fail,
but you want the connector to continue, you can apply @non_critical decorator
which will catch all exceptions and log them, but won't stop the replication.
These functions need to expect and accept a json object.

@data_handler
@non_critical
def append(doc):
    if 'email' in doc:
        doc['email'] += '.com'

"""

import logging
from functools import wraps

logger = logging.getLogger(__name__)


def non_critical(f):
    """Catching all exceptions for non-critical tasks
    in order to continue replicating even on failure
    """
    @wraps(f)
    def catch_all(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.error(e)
    return catch_all


def data_handler(f):
    """Data handler decorator, used to differentiate
    functions which should be applied to docs
    """
    f.is_handler = True
    return f
