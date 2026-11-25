"""CHIMAERA_LOG_FORMAT: the driver prints its calls' output as text or as one JSON object per line."""

import json

from conftest import run_script

SCRIPT = """
import chimaera as ch

@ch.remote
def speak(word):
    print(word, flush=True)
    return word

assert speak.remote("hello-json").result(timeout=60) == "hello-json"
"""


def test_json_lines_carry_the_call_its_process_and_stream():
    r = run_script(SCRIPT, env={"CHIMAERA_LOG_FORMAT": "json"})
    assert r.returncode == 0, r.stderr[-2000:]
    records = [json.loads(line) for line in r.stdout.splitlines() if line.startswith("{")]
    (rec,) = [x for x in records if x.get("line") == "hello-json"]
    assert rec["function"].endswith("speak") and rec["stream"] == "stdout"
    assert rec["pid"] > 0 and len(rec["task"]) == 64 and rec["time"] > 0
    assert "(speak pid=" not in r.stdout


def test_text_is_the_default():
    r = run_script(SCRIPT)
    assert r.returncode == 0, r.stderr[-2000:]
    assert any(line.startswith("(speak pid=") and line.endswith(" hello-json") for line in r.stdout.splitlines())


def test_an_unknown_format_fails_at_init():
    r = run_script(SCRIPT, env={"CHIMAERA_LOG_FORMAT": "xml"})
    assert r.returncode != 0 and "CHIMAERA_LOG_FORMAT='xml'" in r.stderr, r.stderr[-1000:]
