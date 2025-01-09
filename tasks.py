import asyncio
import logging
import websockets
from websockets.exceptions import InvalidStatusCode
import pandas as pd
import streamlit as st
from ghapi.all import GhApi, paged

from celery_utils import app
from celery.schedules import crontab
from persistence import save_leaderboard
from constants import CELERY_TIMEZONE
from constants import IFRAME_PATH
from constants import WS_SUFFIX
from constants import PAGE_LOCATION_FILE
from constants import GITHUB_REPO
from constants import GITHUB_OWNER
import json


logging.basicConfig()
LOGGER = logging.getLogger("tasks")


def get_page_location():
    with open(PAGE_LOCATION_FILE) as json_file:
        return json.load(json_file)


app.conf.beat_schedule = {
    'generate-leaderboard-every-day': {
        'task': 'tasks.compute_leaderboard',
        'schedule': crontab(minute=0),
        'args': []
    },
    'keep-alive': {
        'task': 'tasks.keep_alive',
        'schedule': 60 * 60 * 24,
        'args': [get_page_location()]
    },
}
app.conf.timezone = CELERY_TIMEZONE

api = GhApi(token=st.secrets.github.token)


@app.task
def compute_leaderboard():
    LOGGER.info("Computing Leaderboard")
    all_issues = get_overall_issues()

    # Only query issues which had at least 1 reaction
    issue_numbers = all_issues.query("reactions_total_count > 0").number.unique().tolist()

    reactions_df = get_overall_reactions(issue_numbers)

    save_leaderboard(all_issues, reactions_df)
    LOGGER.info(
        "Leaderboard computed and saved with %s reactions from %s issues",
        len(all_issues),
        len(reactions_df),
    )


async def connect(url, origin, host):
    try:
        async with websockets.connect(
                url,
                user_agent_header="",
                extra_headers={
                    "Host": host,
                    "Origin": origin,
                }
        ) as websocket:
            await websocket.recv()
    except InvalidStatusCode as e:
        LOGGER.info(e)
        LOGGER.info(e.status_code)
        LOGGER.info(e.headers)


@app.task
def keep_alive(location):
    if not location:
        return
    ws_protocol = "ws://" if location["protocol"] == "http:" else "wss://"
    iframe = IFRAME_PATH if IFRAME_PATH in location["pathname"] else ""
    url = f'{ws_protocol}{location["host"]}{iframe}{WS_SUFFIX}'
    LOGGER.info(f"URL: {url}")
    asyncio.run(connect(url, location["origin"], location["host"]))


def get_overall_issues() -> pd.DataFrame:
    LOGGER.info("Getting overall issues")

    # Get raw data
    raw_issues = list()

    pages = paged(
        api.issues.list_for_repo,
        owner=GITHUB_OWNER,
        repo=GITHUB_REPO,
        per_page=100,
        sort="created",
        direction="desc",
        pull_request=False
    )

    LOGGER.info("Iterating pages")
    for page in pages:
        raw_issues += page

    # Parse into a dataframe
    df = pd.json_normalize(raw_issues)

    # Make sure types are properly understood
    df.created_at = pd.to_datetime(df.created_at)
    df.updated_at = pd.to_datetime(df.updated_at)

    # Replace special chars in columns to facilitate access in namedtuples
    df.columns = [
        col.replace(".", "_").replace("+1", "plus1").replace("-1", "minus1")
        for col in df.columns
    ]

    LOGGER.info("Got %s issues", len(df))
    return df


def _get_overall_reactions(issue_number: int):
    # Get raw data
    raw_reactions = list()

    pages = paged(
        api.reactions.list_for_issue,
        owner=GITHUB_OWNER,
        repo=GITHUB_REPO,
        issue_number=issue_number,
        per_page=100,
    )

    for page in pages:
        raw_reactions += page

    # Parse into a dataframe
    reactions_df = pd.json_normalize(raw_reactions)

    LOGGER.info("Got %s reactions for issue %s", len(reactions_df), issue_number)
    return reactions_df


def get_overall_reactions(issue_numbers: list):
    LOGGER.info("Getting overall reactions for %s issues", len(issue_numbers))
    reactions_dfs = list()
    for issue_number in issue_numbers:
        reactions_df = _get_overall_reactions(issue_number)
        if not reactions_df.empty:
            reactions_df = reactions_df[
                ["created_at", "content", "user.login", "user.id", "user.avatar_url"]
            ]
            reactions_df["issue_number"] = issue_number
            reactions_dfs.append(reactions_df)

    all_reactions_df = pd.concat(reactions_dfs)
    LOGGER.info("Got %s overall reactions", len(all_reactions_df))
    return all_reactions_df
