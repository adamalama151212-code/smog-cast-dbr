"""The wheel's console entry point parses arguments without starting Spark."""

import pytest

from smogcast.silver_transform.main import STEPS, main


def test_help_exits_cleanly(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    for step in STEPS:
        assert step in out


def test_unknown_step_is_rejected():
    with pytest.raises(SystemExit) as exc:
        main(["--step", "no-such-step"])
    assert exc.value.code == 2
