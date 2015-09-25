# coding: utf-8
# Copyright 2009-2014 The txmongo authors.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.


class TimeExceeded(Exception):
    """Raised when deadline or timeout for a call has been exceeded.

    .. versionadded:: 15.3.0
    """