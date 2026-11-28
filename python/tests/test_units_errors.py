"""Chimaera's own errors survive pickling: tooling, multiprocessing and a call
that raises another call's error send them on."""

import pickle

from chimaera import errors
from chimaera._errors import RemoteError


def test_a_remote_error_survives_pickling_with_its_fields_and_notes():
    e = RemoteError("ValueError", "boom", "Traceback (most recent call last): ...")
    e.add_note("remote traceback (attempt 2): ...")
    back = pickle.loads(pickle.dumps(e))
    assert type(back) is RemoteError and str(back) == "ValueError: boom"
    assert (back.type_name, back.message, back.remote_traceback) == (e.type_name, e.message, e.remote_traceback)
    assert back.__notes__ == e.__notes__


def test_every_public_error_survives_pickling():
    for name in dir(errors):
        cls = getattr(errors, name)
        if isinstance(cls, type) and issubclass(cls, BaseException) and cls is not RemoteError:
            e = cls("a message")
            assert str(pickle.loads(pickle.dumps(e))) == str(e), name
