from typing import Union, MutableMapping

from bson.raw_bson import RawBSONDocument

Document = Union[MutableMapping, RawBSONDocument]
