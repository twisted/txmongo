from typing import MutableMapping, Union

from bson.raw_bson import RawBSONDocument

Document = Union[MutableMapping, RawBSONDocument]
