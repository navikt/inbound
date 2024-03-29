import errno
import itertools
import os
import random
import re
import tempfile
from contextlib import contextmanager
from pathlib import Path

from inbound.core.logging import LOGGER


@contextmanager
def use_dir(path):
    """
    Utility function to temporarily switch directory
    """
    current_dir = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(current_dir)


def persist_to_target(data: str, target_dir: Path | str, out_file: str) -> Path:
    if target_dir is None:
        target_dir = tempfile.mkdtemp()

    if not isinstance(target_dir, Path):
        target_dir = Path(target_dir)

    try:
        result_file = str(target_dir / out_file)
        LOGGER.info(f"Persisting job_result to {result_file}.")
        with open(result_file, "a+") as log_file:
            log_file.write(data)
    except Exception as e:
        LOGGER.error(f"Error persisting job_result to {result_file}. {e}")

    return Path(target_dir)


def clean_column_names(s):
    """A BigQuery column name must contain only letters (a-z, A-Z),
    numbers (0-9), or underscores (_), and it must start with a letter or underscore.
    The maximum column name length is 300 characters.
    A column name cannot use any of the following prefixes: _TABLE_, _FILE_, _PARTITION

    Args:
        s (string): original column name

    Returns:
        string: string which can be used as a BigQuery column name
    """
    res = re.sub(
        "[^a-zA-Z0-9]+",
        "_",
        s.lower()
        .strip()
        .replace("ø", "o")
        .replace("æ", "ae")
        .replace("å", "aa")
        .replace("_TABLE_", "table_")
        .replace("_FILE_", "file_")
        .replace("_PARTITION", "partition_"),
    ).strip()

    if res[0].isdigit():
        res = "_" + res

    return res[0:300]


def generate_id() -> str:
    random_obj = random.SystemRandom()
    parts = {verbs: 1, adjectives: 1, nouns: 1}
    for _ in range(3, 3):
        parts[random_obj.choice(list(parts.keys()))] += 1
    parts = itertools.chain.from_iterable(
        random_obj.sample(part, count) for part, count in parts.items()
    )
    return "-".join(parts)


nouns = (
    "time",
    "year",
    "people",
    "way",
    "day",
    "man",
    "thing",
    "woman",
    "life",
    "child",
    "world",
    "school",
    "state",
    "family",
    "student",
    "group",
    "country",
    "problem",
    "hand",
    "part",
    "place",
    "case",
    "week",
    "company",
    "system",
    "program",
    "question",
    "work",
    "government",
    "number",
    "night",
    "point",
    "home",
    "water",
    "room",
    "mother",
    "area",
    "money",
    "story",
    "fact",
    "month",
    "lot",
    "right",
    "study",
    "book",
    "eye",
    "job",
    "word",
    "business",
    "issue",
    "side",
    "kind",
    "head",
    "house",
    "service",
    "friend",
    "father",
    "power",
    "hour",
    "game",
    "line",
    "end",
    "member",
    "law",
    "car",
    "city",
    "community",
    "name",
    "president",
    "team",
    "minute",
    "idea",
    "kid",
    "body",
    "information",
    "back",
    "parent",
    "face",
    "others",
    "level",
    "office",
    "door",
    "health",
    "person",
    "art",
    "war",
    "history",
    "party",
    "result",
    "change",
    "morning",
    "reason",
    "research",
    "girl",
    "guy",
    "moment",
    "air",
    "teacher",
    "force",
    "education",
)

adjectives = (
    "other",
    "new",
    "good",
    "high",
    "old",
    "great",
    "big",
    "american",
    "small",
    "large",
    "national",
    "young",
    "different",
    "black",
    "long",
    "little",
    "important",
    "political",
    "bad",
    "white",
    "real",
    "best",
    "right",
    "social",
    "only",
    "public",
    "sure",
    "low",
    "early",
    "able",
    "human",
    "local",
    "late",
    "hard",
    "major",
    "better",
    "economic",
    "strong",
    "possible",
    "whole",
    "free",
    "military",
    "true",
    "federal",
    "international",
    "full",
    "special",
    "easy",
    "clear",
    "recent",
    "certain",
    "personal",
    "open",
    "red",
    "difficult",
    "available",
    "likely",
    "short",
    "single",
    "medical",
    "current",
    "wrong",
    "private",
    "past",
    "foreign",
    "fine",
    "common",
    "poor",
    "natural",
    "significant",
    "similar",
    "hot",
    "dead",
    "central",
    "happy",
    "serious",
    "ready",
    "simple",
    "left",
    "physical",
    "general",
    "environmental",
    "financial",
    "blue",
    "democratic",
    "dark",
    "various",
    "entire",
    "close",
    "legal",
    "religious",
    "cold",
    "final",
    "main",
    "green",
    "nice",
    "huge",
    "popular",
    "traditional",
    "cultural",
    "ludicrous",
)

verbs = (
    "be",
    "have",
    "do",
    "say",
    "go",
    "can",
    "get",
    "would",
    "make",
    "know",
    "will",
    "think",
    "take",
    "see",
    "come",
    "could",
    "want",
    "look",
    "use",
    "find",
    "give",
    "tell",
    "work",
    "may",
    "should",
    "call",
    "try",
    "ask",
    "need",
    "feel",
    "become",
    "leave",
    "put",
    "mean",
    "keep",
    "let",
    "begin",
    "seem",
    "help",
    "talk",
    "turn",
    "start",
    "might",
    "show",
    "hear",
    "play",
    "run",
    "move",
    "like",
    "live",
    "believe",
    "hold",
    "bring",
    "happen",
    "must",
    "write",
    "provide",
    "sit",
    "stand",
    "lose",
    "pay",
    "meet",
    "include",
    "continue",
    "set",
    "learn",
    "change",
    "lead",
    "understand",
    "watch",
    "follow",
    "stop",
    "create",
    "speak",
    "read",
    "allow",
    "add",
    "spend",
    "grow",
    "open",
    "walk",
    "win",
    "offer",
    "remember",
    "love",
    "consider",
    "appear",
    "buy",
    "wait",
    "serve",
    "die",
    "send",
    "expect",
    "build",
    "stay",
    "fall",
    "cut",
    "reach",
    "kill",
    "remain",
)
