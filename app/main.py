from fastapi import FastAPI, Request, Form, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from datetime import datetime
from bson import ObjectId
from botocore.exceptions import BotoCoreError, ClientError
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4
import boto3
import mimetypes
import os
import logging

from app.init.db import db

logger = logging.getLogger(__name__)

PLAYING_XI_COUNT = 9
MAX_WICKETS = PLAYING_XI_COUNT - 1
MAX_BALLS_PER_INNINGS = 36


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting app...")
    await create_indexes()
    yield
    logger.info("Shutting down app...")


app = FastAPI(lifespan=lifespan)

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

AWS_ACCESS_KEY_ID = "AKIAWVKUQO2GM4D53RHZ"
AWS_SECRET_ACCESS_KEY = "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"
AWS_REGION = "us-east-1"
AWS_S3_BUCKET_NAME = "webinarprof"
AWS_S3_BASE_FOLDER = "cricket-manager"

s3_client = None
if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and AWS_S3_BUCKET_NAME:
    s3_client = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


# =========================================================
# Helpers
# =========================================================
def oid(value: str) -> ObjectId:
    try:
        return ObjectId(value)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ID")


def safe_str_id(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if doc and "_id" in doc and not isinstance(doc["_id"], str):
        doc["_id"] = str(doc["_id"])
    return doc


def safe_str_ids(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for doc in docs:
        safe_str_id(doc)
    return docs


def calculate_overs_from_balls(legal_balls: int) -> float:
    return int(legal_balls / 6) + (legal_balls % 6) / 10


def overs_to_decimal(legal_balls: int) -> float:
    return legal_balls / 6 if legal_balls > 0 else 0.0


def parse_object_ids(values: Set[str]) -> List[ObjectId]:
    result = []
    for value in values:
        try:
            result.append(ObjectId(value))
        except Exception:
            continue
    return result


def build_s3_key(folder: str, filename: str) -> str:
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    ext = os.path.splitext(filename)[1].lower()
    unique_name = f"{timestamp}_{uuid4().hex}{ext}"
    folder = folder.strip("/")

    if AWS_S3_BASE_FOLDER:
        return f"{AWS_S3_BASE_FOLDER}/{folder}/{unique_name}"
    return f"{folder}/{unique_name}"


def guess_content_type(filename: str) -> str:
    content_type, _ = mimetypes.guess_type(filename)
    return content_type or "application/octet-stream"


async def upload_file_to_s3(file: UploadFile, folder: str) -> str:
    if not file or not file.filename:
        return ""

    if not s3_client or not AWS_S3_BUCKET_NAME:
        raise RuntimeError("S3 is not configured")

    file.file.seek(0)
    s3_key = build_s3_key(folder, file.filename)
    content_type = file.content_type or guess_content_type(file.filename)

    try:
        s3_client.upload_fileobj(
            Fileobj=file.file,
            Bucket=AWS_S3_BUCKET_NAME,
            Key=s3_key,
            ExtraArgs={"ContentType": content_type},
        )
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"S3 upload failed: {str(exc)}") from exc

    return f"https://{AWS_S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"


async def save_upload(file: Optional[UploadFile], folder: str) -> str:
    if not file or not file.filename:
        return ""
    return await upload_file_to_s3(file, folder)


async def fetch_team_map(team_ids: Set[str]) -> Dict[str, Dict[str, Any]]:
    if not team_ids:
        return {}
    docs = await db.teams.find({"_id": {"$in": parse_object_ids(team_ids)}}).to_list(length=500)
    return {str(doc["_id"]): safe_str_id(doc) for doc in docs}


async def fetch_tournament_map(tournament_ids: Set[str]) -> Dict[str, Dict[str, Any]]:
    if not tournament_ids:
        return {}
    docs = await db.tournaments.find({"_id": {"$in": parse_object_ids(tournament_ids)}}).to_list(length=500)
    return {str(doc["_id"]): safe_str_id(doc) for doc in docs}


async def fetch_player_map(player_ids: Set[str]) -> Dict[str, Dict[str, Any]]:
    if not player_ids:
        return {}
    docs = await db.players.find({"_id": {"$in": parse_object_ids(player_ids)}}).to_list(length=1000)
    return {str(doc["_id"]): safe_str_id(doc) for doc in docs}


async def fetch_live_innings_map(match_ids: Set[str]) -> Dict[str, Dict[str, Any]]:
    if not match_ids:
        return {}
    docs = await db.innings.find({
        "match_id": {"$in": list(match_ids)},
        "innings_status": "live",
    }).to_list(length=500)
    result = {}
    for doc in docs:
        safe_str_id(doc)
        result[doc["match_id"]] = doc
    return result


async def enrich_matches_basic(matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not matches:
        return matches

    safe_str_ids(matches)
    team_ids: Set[str] = set()
    tournament_ids: Set[str] = set()
    match_ids: Set[str] = set()

    for match in matches:
        if match.get("team1_id"):
            team_ids.add(match["team1_id"])
        if match.get("team2_id"):
            team_ids.add(match["team2_id"])
        if match.get("tournament_id"):
            tournament_ids.add(match["tournament_id"])
        if match.get("_id"):
            match_ids.add(match["_id"])

    team_map = await fetch_team_map(team_ids)
    tournament_map = await fetch_tournament_map(tournament_ids)
    live_innings_map = await fetch_live_innings_map(match_ids)

    for match in matches:
        team1 = team_map.get(match.get("team1_id"), {})
        team2 = team_map.get(match.get("team2_id"), {})
        tournament = tournament_map.get(match.get("tournament_id"), {})
        live_innings = live_innings_map.get(match.get("_id"))

        match["team1_name"] = match.get("team1_name") or team1.get("name", "-")
        match["team2_name"] = match.get("team2_name") or team2.get("name", "-")
        match["tournament_name"] = match.get("tournament_name") or tournament.get("name", "-")

        if live_innings:
            match["score"] = f'{live_innings.get("total_runs", 0)}/{live_innings.get("wickets", 0)}'
            match["overs"] = live_innings.get("overs", 0.0)
        else:
            match.setdefault("score", "0/0")
            match.setdefault("overs", 0.0)

    return matches


async def rebuild_innings_state(innings: Dict[str, Any]) -> Dict[str, Any]:
    innings_id = str(innings["_id"])
    balls = await db.balls.find({"innings_id": innings_id}).sort("_id", 1).to_list(length=3000)

    total_runs = 0
    wickets = 0
    legal_balls = 0

    striker_id = innings.get("opening_striker_id") or innings.get("striker_id")
    non_striker_id = innings.get("opening_non_striker_id") or innings.get("non_striker_id")
    current_bowler_id = innings.get("opening_bowler_id") or innings.get("current_bowler_id")

    for ball in balls:
        extra_type = ball.get("extras_type")
        counts_as_legal = ball.get("counts_as_legal_ball")
        if counts_as_legal is None:
            counts_as_legal = extra_type not in ["wide", "noball"]

        runs = ball.get("team_runs_this_ball")
        if runs is None:
            runs = ball.get("runs", 0) + ball.get("extras_runs", 0)

        total_runs += runs

        wicket_type = ball.get("wicket_type")
        if ball.get("is_wicket") and wicket_type != "retiredhurt":
            wickets += 1

        if counts_as_legal:
            legal_balls += 1

        strike_component = ball.get("strike_run_component")
        if strike_component is None:
            strike_component = runs

        dismissed_player_id = ball.get("dismissed_player_id")
        new_batsman_id = ball.get("new_batsman_id")

        if dismissed_player_id and new_batsman_id:
            if dismissed_player_id == striker_id:
                striker_id = new_batsman_id
            elif dismissed_player_id == non_striker_id:
                non_striker_id = new_batsman_id

        if strike_component in [1, 3, 5]:
            striker_id, non_striker_id = non_striker_id, striker_id

        if counts_as_legal and legal_balls % 6 == 0:
            striker_id, non_striker_id = non_striker_id, striker_id

        current_bowler_id = ball.get("bowler_id", current_bowler_id)

    innings_status = innings.get("innings_status", "live")
    if wickets >= MAX_WICKETS or legal_balls >= MAX_BALLS_PER_INNINGS:
        innings_status = "completed"
    elif not balls and not striker_id and not non_striker_id and not current_bowler_id:
        innings_status = "pending"
    else:
        innings_status = "live"

    return {
        "total_runs": total_runs,
        "wickets": wickets,
        "legal_balls": legal_balls,
        "overs": calculate_overs_from_balls(legal_balls),
        "striker_id": striker_id,
        "non_striker_id": non_striker_id,
        "current_bowler_id": current_bowler_id,
        "innings_status": innings_status,
    }


async def sync_match_state(match_id: str) -> None:
    innings_list = await db.innings.find({"match_id": match_id}).sort("innings_number", 1).to_list(length=10)
    match = await db.matches.find_one({"_id": oid(match_id)})
    if not match:
        return

    target = None
    winner_team_id = None
    match_result = None
    result_text = match.get("result_text", "")

    if innings_list:
        first_innings = next((inn for inn in innings_list if inn.get("innings_number") == 1), None)
        if first_innings and first_innings.get("innings_status") == "completed":
            target = first_innings.get("total_runs", 0) + 1

        second_innings = next((inn for inn in innings_list if inn.get("innings_number") == 2), None)
        if second_innings and second_innings.get("innings_status") == "completed":
            team1_score = 0
            team2_score = 0
            for inn in innings_list:
                if inn.get("batting_team_id") == match.get("team1_id"):
                    team1_score = inn.get("total_runs", 0)
                elif inn.get("batting_team_id") == match.get("team2_id"):
                    team2_score = inn.get("total_runs", 0)

            team_map = await fetch_team_map({match["team1_id"], match["team2_id"]})
            team1_name = team_map.get(match["team1_id"], {}).get("name", match.get("team1_name", "Team 1"))
            team2_name = team_map.get(match["team2_id"], {}).get("name", match.get("team2_name", "Team 2"))

            if team1_score > team2_score:
                winner_team_id = match["team1_id"]
                match_result = "team1"
                result_text = f"{team1_name} won the match | {team1_name}: {team1_score} | {team2_name}: {team2_score}"
            elif team2_score > team1_score:
                winner_team_id = match["team2_id"]
                match_result = "team2"
                result_text = f"{team2_name} won the match | {team1_name}: {team1_score} | {team2_name}: {team2_score}"
            else:
                match_result = "tied"
                result_text = f"Match tied | {team1_name}: {team1_score} | {team2_name}: {team2_score}"

    update_fields: Dict[str, Any] = {"target": target}
    if match_result:
        update_fields.update({
            "status": "completed",
            "winner_team_id": winner_team_id,
            "match_result": match_result,
            "result_text": result_text,
        })
    else:
        update_fields.update({
            "winner_team_id": None,
            "match_result": None,
        })
        if any(inn.get("innings_status") == "live" for inn in innings_list):
            update_fields["status"] = "live"
        else:
            update_fields["status"] = match.get("status", "upcoming")

    await db.matches.update_one({"_id": oid(match_id)}, {"$set": update_fields})


async def create_second_innings_if_missing(match_id: str, first_innings: Dict[str, Any]) -> None:
    second_innings = await db.innings.find_one({"match_id": match_id, "innings_number": 2})
    if second_innings:
        return

    await db.innings.insert_one({
        "match_id": match_id,
        "innings_number": 2,
        "batting_team_id": first_innings["bowling_team_id"],
        "bowling_team_id": first_innings["batting_team_id"],
        "total_runs": 0,
        "wickets": 0,
        "legal_balls": 0,
        "overs": 0.0,
        "striker_id": None,
        "non_striker_id": None,
        "current_bowler_id": None,
        "opening_striker_id": None,
        "opening_non_striker_id": None,
        "opening_bowler_id": None,
        "innings_status": "pending",
    })


def get_counts_as_legal(extra_type: Optional[str], explicit_value: Optional[bool] = None) -> bool:
    if explicit_value is not None:
        return explicit_value
    return extra_type not in ["wide", "noball"]


async def validate_match_player_roles(match: Dict[str, Any], batting_team_id: str, bowling_team_id: str,
                                      striker_id: str, non_striker_id: str, current_bowler_id: str) -> Optional[HTMLResponse]:
    if striker_id == non_striker_id:
        return HTMLResponse("Striker and non-striker must be different", status_code=400)

    batting_players = await db.players.count_documents({
        "_id": {"$in": [oid(striker_id), oid(non_striker_id)]},
        "team_id": batting_team_id,
        "is_playing": True,
    })
    if batting_players != 2:
        return HTMLResponse("Invalid batting players selected", status_code=400)

    bowler = await db.players.find_one({
        "_id": oid(current_bowler_id),
        "team_id": bowling_team_id,
        "is_playing": True,
    })
    if not bowler:
        return HTMLResponse("Invalid bowler selected", status_code=400)

    return None


# =========================================================
# Additional Scoring Helpers
# =========================================================
async def get_current_or_latest_innings(match_id: str) -> Optional[Dict[str, Any]]:
    live_or_pending = await db.innings.find_one(
        {"match_id": match_id, "innings_status": {"$in": ["live", "pending"]}},
        sort=[("innings_number", -1)],
    )
    if live_or_pending:
        return live_or_pending

    latest_completed = await db.innings.find_one({"match_id": match_id}, sort=[("innings_number", -1)])
    return latest_completed


async def build_innings_scorecard(innings_doc: Dict[str, Any]) -> Dict[str, Any]:
    innings_id = str(innings_doc["_id"])
    balls = await db.balls.find({"innings_id": innings_id}).sort("_id", 1).to_list(length=3000)

    batting_stats: Dict[str, Dict[str, Any]] = {}
    bowling_stats: Dict[str, Dict[str, Any]] = {}
    dismissed_ids: Set[str] = set()
    all_player_ids: Set[str] = set()

    for ball in balls:
        batsman_id = ball.get("batsman_id")
        bowler_id = ball.get("bowler_id")
        fielder_id = ball.get("fielder_id")
        extra_type = ball.get("extras_type")

        if batsman_id:
            all_player_ids.add(batsman_id)
        if bowler_id:
            all_player_ids.add(bowler_id)
        if fielder_id:
            all_player_ids.add(fielder_id)

        bat_runs = ball.get("bat_runs", ball.get("runs", 0))
        counts_as_legal = get_counts_as_legal(extra_type, ball.get("counts_as_legal_ball"))

        bowler_runs_this_ball = ball.get("bowler_runs_this_ball")
        if bowler_runs_this_ball is None:
            bowler_runs_this_ball = ball.get("runs", 0) + ball.get("extras_runs", 0)

        if batsman_id:
            batting_stats.setdefault(batsman_id, {
                "runs": 0,
                "balls": 0,
                "fours": 0,
                "sixes": 0,
                "out": False,
                "dismissal_text": "not out",
            })
            batting_stats[batsman_id]["runs"] += bat_runs
            if counts_as_legal:
                batting_stats[batsman_id]["balls"] += 1
            if bat_runs == 4:
                batting_stats[batsman_id]["fours"] += 1
            if bat_runs == 6:
                batting_stats[batsman_id]["sixes"] += 1

        if bowler_id:
            bowling_stats.setdefault(bowler_id, {
                "runs": 0,
                "legal_balls": 0,
                "wickets": 0,
            })
            bowling_stats[bowler_id]["runs"] += bowler_runs_this_ball
            if counts_as_legal:
                bowling_stats[bowler_id]["legal_balls"] += 1
            if ball.get("is_wicket") and ball.get("bowler_wicket_credit"):
                bowling_stats[bowler_id]["wickets"] += 1

    player_map = await fetch_player_map(all_player_ids)

    for ball in balls:
        dismissed_player_id = ball.get("dismissed_player_id")
        wicket_type = ball.get("wicket_type")
        fielder_id = ball.get("fielder_id")
        bowler_id = ball.get("bowler_id")

        if ball.get("is_wicket") and dismissed_player_id:
            if dismissed_player_id in batting_stats:
                batting_stats[dismissed_player_id]["out"] = wicket_type != "retiredhurt"

                bowler_name = player_map.get(bowler_id, {}).get("name", "") if bowler_id else ""
                fielder_name = player_map.get(fielder_id, {}).get("name", "") if fielder_id else ""

                dismissal_text = "out"
                if wicket_type == "bowled":
                    dismissal_text = f"b {bowler_name}" if bowler_name else "bowled"
                elif wicket_type == "caught":
                    if fielder_name and bowler_name:
                        dismissal_text = f"c {fielder_name} b {bowler_name}"
                    elif bowler_name:
                        dismissal_text = f"c & b {bowler_name}"
                    else:
                        dismissal_text = "caught"
                elif wicket_type == "runout":
                    dismissal_text = f"run out ({fielder_name})" if fielder_name else "run out"
                elif wicket_type == "stumped":
                    if fielder_name and bowler_name:
                        dismissal_text = f"st {fielder_name} b {bowler_name}"
                    elif bowler_name:
                        dismissal_text = f"stumped b {bowler_name}"
                    else:
                        dismissal_text = "stumped"
                elif wicket_type == "sixout":
                    dismissal_text = "six-out"
                elif wicket_type == "retiredhurt":
                    dismissal_text = "retired hurt"

                batting_stats[dismissed_player_id]["dismissal_text"] = dismissal_text

            if wicket_type != "retiredhurt":
                dismissed_ids.add(dismissed_player_id)

    batting_scorecard = []
    for player_id, stats in batting_stats.items():
        player = player_map.get(player_id, {})
        batting_scorecard.append({
            "player_id": player_id,
            "player_name": player.get("name", "-"),
            "runs": stats["runs"],
            "balls": stats["balls"],
            "fours": stats["fours"],
            "sixes": stats["sixes"],
            "out": stats["out"],
            "dismissal_text": stats.get("dismissal_text", "not out"),
            "strike_rate": round((stats["runs"] / stats["balls"] * 100), 2) if stats["balls"] > 0 else 0.0,
        })

    bowling_scorecard = []
    for player_id, stats in bowling_stats.items():
        player = player_map.get(player_id, {})
        bowling_scorecard.append({
            "player_id": player_id,
            "player_name": player.get("name", "-"),
            "overs": calculate_overs_from_balls(stats["legal_balls"]),
            "runs": stats["runs"],
            "wickets": stats["wickets"],
            "economy": round((stats["runs"] / overs_to_decimal(stats["legal_balls"])), 2) if stats["legal_balls"] > 0 else 0.0,
        })

    batting_scorecard.sort(key=lambda x: (-x["runs"], x["balls"]))
    bowling_scorecard.sort(key=lambda x: (-x["wickets"], x["runs"]))

    return {
        "innings": safe_str_id(innings_doc),
        "batting_scorecard": batting_scorecard,
        "bowling_scorecard": bowling_scorecard,
        "dismissed_batsman_ids": list(dismissed_ids),
    }


async def build_tournament_analytics(tournament_id: str) -> Dict[str, Any]:
    matches = await db.matches.find({"tournament_id": tournament_id}).to_list(length=500)
    match_ids = [str(m["_id"]) for m in matches]
    if not match_ids:
        return {"top_batters": [], "top_bowlers": [], "top_fielders": []}

    innings_docs = await db.innings.find({"match_id": {"$in": match_ids}}).to_list(length=1000)
    innings_ids = [str(i["_id"]) for i in innings_docs]
    if not innings_ids:
        return {"top_batters": [], "top_bowlers": [], "top_fielders": []}

    balls = await db.balls.find({"innings_id": {"$in": innings_ids}}).to_list(length=10000)

    batting: Dict[str, Dict[str, Any]] = {}
    bowling: Dict[str, Dict[str, Any]] = {}
    fielding: Dict[str, Dict[str, Any]] = {}
    player_ids: Set[str] = set()

    for ball in balls:
        batsman_id = ball.get("batsman_id")
        bowler_id = ball.get("bowler_id")
        fielder_id = ball.get("fielder_id")
        extra_type = ball.get("extras_type")

        if batsman_id:
            player_ids.add(batsman_id)
            batting.setdefault(batsman_id, {"runs": 0, "balls": 0})
            bat_runs = ball.get("bat_runs", ball.get("runs", 0))
            batting[batsman_id]["runs"] += bat_runs
            if get_counts_as_legal(extra_type, ball.get("counts_as_legal_ball")):
                batting[batsman_id]["balls"] += 1

        if bowler_id:
            player_ids.add(bowler_id)
            bowling.setdefault(bowler_id, {"runs": 0, "wickets": 0, "legal_balls": 0})
            bowler_runs_this_ball = ball.get("bowler_runs_this_ball")
            if bowler_runs_this_ball is None:
                bowler_runs_this_ball = ball.get("runs", 0) + ball.get("extras_runs", 0)
            bowling[bowler_id]["runs"] += bowler_runs_this_ball

            if get_counts_as_legal(extra_type, ball.get("counts_as_legal_ball")):
                bowling[bowler_id]["legal_balls"] += 1

            if ball.get("is_wicket") and ball.get("bowler_wicket_credit"):
                bowling[bowler_id]["wickets"] += 1

        if fielder_id:
            player_ids.add(fielder_id)
            fielding.setdefault(fielder_id, {"catches": 0, "runouts": 0, "stumpings": 0})
            wicket_type = ball.get("wicket_type")
            if wicket_type == "caught":
                fielding[fielder_id]["catches"] += 1
            elif wicket_type == "runout":
                fielding[fielder_id]["runouts"] += 1
            elif wicket_type == "stumped":
                fielding[fielder_id]["stumpings"] += 1

    player_map = await fetch_player_map(player_ids)

    top_batters = [{
        "name": player_map.get(pid, {}).get("name", "-"),
        "runs": stats["runs"],
        "strike_rate": round((stats["runs"] / stats["balls"]) * 100, 2) if stats["balls"] > 0 else 0.0,
    } for pid, stats in batting.items()]
    top_batters.sort(key=lambda x: (-x["runs"], -x["strike_rate"]))

    top_bowlers = [{
        "name": player_map.get(pid, {}).get("name", "-"),
        "wickets": stats["wickets"],
        "economy": round(stats["runs"] / overs_to_decimal(stats["legal_balls"]), 2) if stats["legal_balls"] > 0 else 0.0,
    } for pid, stats in bowling.items()]
    top_bowlers.sort(key=lambda x: (-x["wickets"], x["economy"]))

    top_fielders = [{
        "name": player_map.get(pid, {}).get("name", "-"),
        "catches": stats["catches"],
        "runouts": stats["runouts"],
        "stumpings": stats["stumpings"],
    } for pid, stats in fielding.items()]
    top_fielders.sort(key=lambda x: (-(x["catches"] + x["runouts"] + x["stumpings"]), -x["catches"]))

    return {
        "top_batters": top_batters[:3],
        "top_bowlers": top_bowlers[:3],
        "top_fielders": top_fielders[:3],
    }


async def create_indexes() -> None:
    try:
        await db.tournaments.create_index([("status", 1), ("created_at", -1)])
        await db.teams.create_index([("tournament_id", 1), ("name", 1)])
        await db.players.create_index([("team_id", 1), ("is_playing", 1), ("name", 1)])
        await db.matches.create_index([("tournament_id", 1), ("status", 1), ("match_date", 1), ("match_time", 1)])
        await db.matches.create_index([("status", 1), ("match_date", 1)])
        await db.matches.create_index([("team1_id", 1), ("team2_id", 1), ("match_date", 1), ("match_time", 1)])
        await db.innings.create_index([("match_id", 1), ("innings_number", 1)], unique=True)
        await db.innings.create_index([("match_id", 1), ("innings_status", 1)])
        await db.balls.create_index([("innings_id", 1), ("over_number", 1), ("ball_number", 1)])
        await db.balls.create_index([("innings_id", 1), ("counts_as_legal_ball", 1)])
        await db.balls.create_index([("batsman_id", 1)])
        await db.balls.create_index([("bowler_id", 1)])
        await db.balls.create_index([("fielder_id", 1)])
    except Exception as exc:
        logger.exception("Failed to create indexes: %s", exc)


# =========================================================
# Home
# =========================================================
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    live_matches = await db.matches.find({"status": "live"}).sort("_id", -1).to_list(length=10)
    upcoming_matches = await db.matches.find({"status": "upcoming"}).sort("_id", -1).to_list(length=10)
    recent_matches = await db.matches.find({"status": "completed"}).sort("_id", -1).to_list(length=10)

    live_matches = await enrich_matches_basic(live_matches)
    upcoming_matches = await enrich_matches_basic(upcoming_matches)
    recent_matches = await enrich_matches_basic(recent_matches)

    for match in recent_matches:
        if not match.get("result_text"):
            match["result_text"] = f'{match.get("team1_name", "-")} vs {match.get("team2_name", "-")}'

    tournaments = await db.tournaments.find().sort("_id", -1).to_list(length=20)
    safe_str_ids(tournaments)

    tournament_ids = [t["_id"] for t in tournaments]
    counts_map: Dict[str, Dict[str, int]] = {}
    if tournament_ids:
        pipeline = [
            {"$match": {"tournament_id": {"$in": tournament_ids}}},
            {"$group": {"_id": {"tournament_id": "$tournament_id", "status": "$status"}, "count": {"$sum": 1}}},
        ]
        grouped = await db.matches.aggregate(pipeline).to_list(length=500)
        for row in grouped:
            tid = row["_id"]["tournament_id"]
            status = row["_id"]["status"]
            counts_map.setdefault(tid, {})[status] = row["count"]

    for tournament in tournaments:
        tournament["live_count"] = counts_map.get(tournament["_id"], {}).get("live", 0)
        tournament["upcoming_count"] = counts_map.get(tournament["_id"], {}).get("upcoming", 0)

    tournament_analytics = []
    for tournament in tournaments[:4]:
        analytics = await build_tournament_analytics(tournament["_id"])
        tournament_analytics.append({
            "tournament_name": tournament["name"],
            "top_batters": analytics["top_batters"],
            "top_bowlers": analytics["top_bowlers"],
            "top_fielders": analytics["top_fielders"],
        })

    return templates.TemplateResponse(
        request=request,
        name="home.html",
        context={
            "live_matches": live_matches,
            "upcoming_matches": upcoming_matches,
            "recent_matches": recent_matches,
            "tournaments": tournaments,
            "tournament_analytics": tournament_analytics,
        },
    )


# =========================================================
# Tournaments
# =========================================================
@app.get("/teams/{team_id}/view", response_class=HTMLResponse)
async def team_view(request: Request, team_id: str):
    team = await db.teams.find_one({"_id": oid(team_id)})
    if not team:
        return HTMLResponse("Team not found", status_code=404)

    team = safe_str_id(team)

    tournament = None
    if team.get("tournament_id"):
        tournament = await db.tournaments.find_one({"_id": oid(team["tournament_id"])})
        tournament = safe_str_id(tournament) if tournament else None

    players = await db.players.find({"team_id": team_id}).sort("name", 1).to_list(length=100)
    safe_str_ids(players)

    matches = await db.matches.find({
        "$or": [{"team1_id": team_id}, {"team2_id": team_id}],
    }).sort("match_date", -1).to_list(length=200)
    safe_str_ids(matches)

    total_played = 0
    total_wins = 0
    total_losses = 0
    total_tied = 0
    total_abandoned = 0
    recent_results = []

    for match in matches:
        if match.get("status") != "completed":
            continue

        total_played += 1
        winner_team_id = match.get("winner_team_id")
        match_result = match.get("match_result")

        if match_result == "tied":
            total_tied += 1
        elif match_result == "abandoned":
            total_abandoned += 1
        elif winner_team_id == team_id:
            total_wins += 1
        else:
            total_losses += 1

        recent_results.append({
            "_id": match["_id"],
            "team1_name": match.get("team1_name", "-"),
            "team2_name": match.get("team2_name", "-"),
            "match_date": match.get("match_date", "-"),
            "round_type": match.get("round_type", "-"),
            "result_text": match.get("result_text", "Result not available"),
            "status": match.get("status", "-"),
        })

    recent_results = recent_results[:8]
    playing_count = sum(1 for p in players if p.get("is_playing"))
    total_players = len(players)
    win_percentage = round((total_wins / total_played) * 100, 2) if total_played > 0 else 0.0

    return templates.TemplateResponse(
        request=request,
        name="team_view.html",
        context={
            "team": team,
            "tournament": tournament,
            "players": players,
            "total_players": total_players,
            "playing_count": playing_count,
            "total_played": total_played,
            "total_wins": total_wins,
            "total_losses": total_losses,
            "total_tied": total_tied,
            "total_abandoned": total_abandoned,
            "win_percentage": win_percentage,
            "recent_results": recent_results,
        },
    )


@app.get("/tournaments", response_class=HTMLResponse)
async def tournaments_page(request: Request):
    tournaments = await db.tournaments.find().sort("_id", -1).to_list(length=100)
    safe_str_ids(tournaments)
    return templates.TemplateResponse(request=request, name="tournaments.html", context={"tournaments": tournaments})


@app.get("/tournaments/create", response_class=HTMLResponse)
async def create_tournament_page(request: Request):
    return templates.TemplateResponse(request=request, name="tournament_create.html", context={})


@app.post("/tournaments/create")
async def create_tournament(
    name: str = Form(...),
    match_date: str = Form(...),
    logo: UploadFile = File(None),
):
    logo_path = await save_upload(logo, "tournaments")

    tournament_doc = {
        "name": name,
        "match_date": match_date,
        "logo": logo_path,
        "status": "active",
        "created_at": datetime.utcnow(),
        "top_batters": [],
        "top_bowlers": [],
        "top_fielders": [],
    }
    await db.tournaments.insert_one(tournament_doc)
    return RedirectResponse(url="/tournaments", status_code=303)


@app.get("/tournaments/{tournament_id}/view", response_class=HTMLResponse)
async def view_tournament(request: Request, tournament_id: str):
    tournament = await db.tournaments.find_one({"_id": oid(tournament_id)})
    if not tournament:
        return HTMLResponse("Tournament not found", status_code=404)

    tournament = safe_str_id(tournament)
    teams = await db.teams.find({"tournament_id": tournament_id}).sort("name", 1).to_list(length=100)
    safe_str_ids(teams)
    return templates.TemplateResponse(request=request, name="tournament_view.html", context={"tournament": tournament, "teams": teams})


@app.get("/tournaments/{tournament_id}/edit", response_class=HTMLResponse)
async def edit_tournament_page(request: Request, tournament_id: str):
    tournament = await db.tournaments.find_one({"_id": oid(tournament_id)})
    if not tournament:
        return HTMLResponse("Tournament not found", status_code=404)

    tournament = safe_str_id(tournament)
    return templates.TemplateResponse(request=request, name="tournament_edit.html", context={"tournament": tournament})


@app.post("/tournaments/{tournament_id}/edit")
async def update_tournament(
    tournament_id: str,
    name: str = Form(...),
    match_date: str = Form(...),
    logo: UploadFile = File(None),
):
    tournament = await db.tournaments.find_one({"_id": oid(tournament_id)})
    if not tournament:
        return HTMLResponse("Tournament not found", status_code=404)

    update_data = {"name": name, "match_date": match_date}
    logo_path = await save_upload(logo, "tournaments")
    if logo_path:
        update_data["logo"] = logo_path

    await db.tournaments.update_one({"_id": oid(tournament_id)}, {"$set": update_data})
    return RedirectResponse(url="/tournaments", status_code=303)


# =========================================================
# Teams
# =========================================================
@app.get("/tournaments/{tournament_id}/teams", response_class=HTMLResponse)
async def tournament_teams_page(request: Request, tournament_id: str):
    tournament = await db.tournaments.find_one({"_id": oid(tournament_id)})
    if not tournament:
        return HTMLResponse("Tournament not found", status_code=404)

    tournament = safe_str_id(tournament)
    teams = await db.teams.find({"tournament_id": tournament_id}).sort("name", 1).to_list(length=100)
    safe_str_ids(teams)
    return templates.TemplateResponse(request=request, name="teams.html", context={"tournament": tournament, "teams": teams})


@app.get("/tournaments/{tournament_id}/teams/create", response_class=HTMLResponse)
async def create_team_page(request: Request, tournament_id: str):
    tournament = await db.tournaments.find_one({"_id": oid(tournament_id)})
    if not tournament:
        return HTMLResponse("Tournament not found", status_code=404)

    tournament = safe_str_id(tournament)
    return templates.TemplateResponse(request=request, name="team_create.html", context={"tournament": tournament})


@app.post("/tournaments/{tournament_id}/teams/create")
async def create_team(
    tournament_id: str,
    name: str = Form(...),
    coach_name: str = Form(...),
    coach_contact: str = Form(...),
    manager_name: str = Form(...),
    manager_contact: str = Form(...),
    team_icon: UploadFile = File(None),
):
    icon_path = await save_upload(team_icon, "teams")

    team_doc = {
        "name": name,
        "team_icon": icon_path,
        "coach": {"name": coach_name, "contact": coach_contact},
        "manager": {"name": manager_name, "contact": manager_contact},
        "tournament_id": tournament_id,
    }
    await db.teams.insert_one(team_doc)
    return RedirectResponse(url=f"/tournaments/{tournament_id}/teams", status_code=303)


@app.get("/teams/{team_id}/edit", response_class=HTMLResponse)
async def edit_team_page(request: Request, team_id: str):
    team = await db.teams.find_one({"_id": oid(team_id)})
    if not team:
        return HTMLResponse("Team not found", status_code=404)

    team = safe_str_id(team)
    tournament = await db.tournaments.find_one({"_id": oid(team["tournament_id"])})
    tournament = safe_str_id(tournament) if tournament else None
    return templates.TemplateResponse(request=request, name="team_edit.html", context={"team": team, "tournament": tournament})


@app.post("/teams/{team_id}/edit")
async def update_team(
    team_id: str,
    name: str = Form(...),
    coach_name: str = Form(...),
    coach_contact: str = Form(...),
    manager_name: str = Form(...),
    manager_contact: str = Form(...),
    team_icon: UploadFile = File(None),
):
    team = await db.teams.find_one({"_id": oid(team_id)})
    if not team:
        return HTMLResponse("Team not found", status_code=404)

    update_data = {
        "name": name,
        "coach": {"name": coach_name, "contact": coach_contact},
        "manager": {"name": manager_name, "contact": manager_contact},
    }

    icon_path = await save_upload(team_icon, "teams")
    if icon_path:
        update_data["team_icon"] = icon_path

    await db.teams.update_one({"_id": oid(team_id)}, {"$set": update_data})
    return RedirectResponse(url=f"/tournaments/{team['tournament_id']}/teams", status_code=303)


@app.get("/teams", response_class=HTMLResponse)
async def all_teams(request: Request, search: str = ""):
    query = {}
    if search:
        query["name"] = {"$regex": search, "$options": "i"}

    teams = await db.teams.find(query).sort("name", 1).to_list(length=100)
    safe_str_ids(teams)

    tournament_ids = {team["tournament_id"] for team in teams if team.get("tournament_id")}
    tournament_map = await fetch_tournament_map(tournament_ids)

    for team in teams:
        tournament = tournament_map.get(team.get("tournament_id"), {})
        team["tournament_name"] = tournament.get("name", "")

    return templates.TemplateResponse(request=request, name="teams_all.html", context={"teams": teams, "search": search})


# =========================================================
# Players
# =========================================================
@app.get("/teams/{team_id}/players", response_class=HTMLResponse)
async def team_players(request: Request, team_id: str, error: str = ""):
    team = await db.teams.find_one({"_id": oid(team_id)})
    if not team:
        return HTMLResponse("Team not found", status_code=404)

    team = safe_str_id(team)
    tournament = await db.tournaments.find_one({"_id": oid(team["tournament_id"])})
    tournament = safe_str_id(tournament) if tournament else None
    players = await db.players.find({"team_id": team_id}).sort("name", 1).to_list(length=100)
    safe_str_ids(players)

    total_players = len(players)
    playing_count = sum(1 for p in players if p.get("is_playing"))
    substitute_count = total_players - playing_count

    return templates.TemplateResponse(
        request=request,
        name="team_players.html",
        context={
            "team": team,
            "tournament": tournament,
            "players": players,
            "total_players": total_players,
            "playing_count": playing_count,
            "substitute_count": substitute_count,
            "error": error,
        },
    )


@app.post("/teams/{team_id}/players/add")
async def add_player(
    team_id: str,
    name: str = Form(...),
    roles: List[str] = Form([]),
    photo: UploadFile = File(None),
    icon: UploadFile = File(None),
):
    photo_path = await save_upload(photo, "players")
    icon_path = await save_upload(icon, "players")

    player_doc = {
        "name": name,
        "photo": photo_path,
        "icon": icon_path,
        "roles": roles,
        "is_playing": False,
        "team_id": team_id,
    }
    await db.players.insert_one(player_doc)
    return RedirectResponse(url=f"/teams/{team_id}/players", status_code=303)


@app.post("/players/{player_id}/toggle-playing")
async def toggle_playing(player_id: str):
    player = await db.players.find_one({"_id": oid(player_id)})
    if not player:
        return HTMLResponse("Player not found", status_code=404)

    current_status = player.get("is_playing", False)
    if current_status:
        await db.players.update_one({"_id": oid(player_id)}, {"$set": {"is_playing": False}})
        return RedirectResponse(url=f"/teams/{player['team_id']}/players", status_code=303)

    playing_count = await db.players.count_documents({"team_id": player["team_id"], "is_playing": True})
    if playing_count >= PLAYING_XI_COUNT:
        return RedirectResponse(url=f"/teams/{player['team_id']}/players?error=max_playing", status_code=303)

    await db.players.update_one({"_id": oid(player_id)}, {"$set": {"is_playing": True}})
    return RedirectResponse(url=f"/teams/{player['team_id']}/players", status_code=303)


@app.post("/players/{player_id}/delete")
async def delete_player(player_id: str):
    player = await db.players.find_one({"_id": oid(player_id)})
    if not player:
        return HTMLResponse("Player not found", status_code=404)

    await db.players.delete_one({"_id": oid(player_id)})
    return RedirectResponse(url=f"/teams/{player['team_id']}/players", status_code=303)


@app.get("/players/{player_id}", response_class=HTMLResponse)
async def player_profile(request: Request, player_id: str):
    player = await db.players.find_one({"_id": oid(player_id)})
    if not player:
        return HTMLResponse("Player not found", status_code=404)

    player = safe_str_id(player)
    team = await db.teams.find_one({"_id": oid(player["team_id"])}) if player.get("team_id") else None
    team = safe_str_id(team) if team else None

    return templates.TemplateResponse(request=request, name="player_profile.html", context={"player": player, "team": team})


# =========================================================
# Matches
# =========================================================
@app.get("/matches/new", response_class=HTMLResponse)
async def new_match_page(request: Request):
    tournaments = await db.tournaments.find().sort("_id", -1).to_list(length=100)
    safe_str_ids(tournaments)
    return templates.TemplateResponse(
        request=request,
        name="new_match.html",
        context={"tournaments": tournaments, "selected_tournament": None},
    )


@app.get("/matches/new/{tournament_id}", response_class=HTMLResponse)
async def tournament_fixtures(request: Request, tournament_id: str, error: str = "", success: str = ""):
    tournament = await db.tournaments.find_one({"_id": oid(tournament_id)})
    if not tournament:
        return HTMLResponse("Tournament not found", status_code=404)

    tournament = safe_str_id(tournament)
    teams = await db.teams.find({"tournament_id": tournament_id}).sort("name", 1).to_list(length=100)
    fixtures = await db.matches.find({"tournament_id": tournament_id}).sort("match_date", 1).to_list(length=100)
    safe_str_ids(teams)
    fixtures = await enrich_matches_basic(fixtures)

    return templates.TemplateResponse(
        request=request,
        name="new_match.html",
        context={
            "selected_tournament": tournament,
            "teams": teams,
            "fixtures": fixtures,
            "error": error,
            "success": success,
        },
    )


@app.post("/matches/create")
async def create_match(
    tournament_id: str = Form(...),
    team1_id: str = Form(...),
    team2_id: str = Form(...),
    match_date: str = Form(...),
    match_time: str = Form(...),
    round_type: str = Form(...),
):
    if team1_id == team2_id:
        return RedirectResponse(url=f"/matches/new/{tournament_id}?error=same_team", status_code=303)

    teams_count = await db.teams.count_documents({"tournament_id": tournament_id})
    if teams_count < 2:
        return RedirectResponse(url=f"/matches/new/{tournament_id}?error=not_enough_teams", status_code=303)

    existing = await db.matches.find_one({
        "tournament_id": tournament_id,
        "team1_id": team1_id,
        "team2_id": team2_id,
        "match_date": match_date,
        "match_time": match_time,
    })
    if existing:
        return RedirectResponse(url=f"/matches/new/{tournament_id}?error=duplicate_fixture", status_code=303)

    team_map = await fetch_team_map({team1_id, team2_id})
    tournament_map = await fetch_tournament_map({tournament_id})

    match_doc = {
        "tournament_id": tournament_id,
        "team1_id": team1_id,
        "team2_id": team2_id,
        "team1_name": team_map.get(team1_id, {}).get("name", ""),
        "team2_name": team_map.get(team2_id, {}).get("name", ""),
        "tournament_name": tournament_map.get(tournament_id, {}).get("name", ""),
        "match_date": match_date,
        "match_time": match_time,
        "round_type": round_type,
        "status": "upcoming",
    }
    await db.matches.insert_one(match_doc)
    return RedirectResponse(url=f"/matches/new/{tournament_id}?success=fixture_created", status_code=303)


@app.post("/matches/{match_id}/delete")
async def delete_match(match_id: str):
    match = await db.matches.find_one({"_id": oid(match_id)})
    if not match:
        return HTMLResponse("Match not found", status_code=404)

    await db.matches.delete_one({"_id": oid(match_id)})
    return RedirectResponse(url=f"/matches/new/{match['tournament_id']}", status_code=303)


@app.get("/matches/{match_id}/edit", response_class=HTMLResponse)
async def edit_match_page(request: Request, match_id: str):
    match = await db.matches.find_one({"_id": oid(match_id)})
    if not match:
        return HTMLResponse("Match not found", status_code=404)

    match = safe_str_id(match)
    teams = await db.teams.find({"tournament_id": match["tournament_id"]}).sort("name", 1).to_list(length=100)
    safe_str_ids(teams)
    return templates.TemplateResponse(request=request, name="match_edit.html", context={"match": match, "teams": teams})


@app.post("/matches/{match_id}/edit")
async def update_match(
    match_id: str,
    team1_id: str = Form(...),
    team2_id: str = Form(...),
    match_date: str = Form(...),
    match_time: str = Form(...),
    round_type: str = Form(...),
):
    match = await db.matches.find_one({"_id": oid(match_id)})
    if not match:
        return HTMLResponse("Match not found", status_code=404)

    if team1_id == team2_id:
        return RedirectResponse(url=f"/matches/{match_id}/edit", status_code=303)

    team_map = await fetch_team_map({team1_id, team2_id})
    await db.matches.update_one(
        {"_id": oid(match_id)},
        {"$set": {
            "team1_id": team1_id,
            "team2_id": team2_id,
            "team1_name": team_map.get(team1_id, {}).get("name", ""),
            "team2_name": team_map.get(team2_id, {}).get("name", ""),
            "match_date": match_date,
            "match_time": match_time,
            "round_type": round_type,
        }},
    )
    return RedirectResponse(url=f"/matches/new/{match['tournament_id']}", status_code=303)


@app.get("/live-match", response_class=HTMLResponse)
async def live_match_page(request: Request, match_date: str = "", team_search: str = "", status: str = ""):
    query = {}
    if match_date:
        query["match_date"] = match_date
    if status:
        query["status"] = status

    matches = await db.matches.find(query).sort("_id", -1).to_list(length=200)
    matches = await enrich_matches_basic(matches)

    filtered_matches = []
    for match in matches:
        if team_search:
            search_text = team_search.lower()
            if search_text not in match.get("team1_name", "").lower() and search_text not in match.get("team2_name", "").lower():
                continue
        filtered_matches.append(match)

    live_matches = [m for m in filtered_matches if m.get("status") == "live"]
    upcoming_matches = [m for m in filtered_matches if m.get("status") == "upcoming"]

    return templates.TemplateResponse(
        request=request,
        name="live_match.html",
        context={
            "live_matches": live_matches,
            "upcoming_matches": upcoming_matches,
            "match_date": match_date,
            "team_search": team_search,
            "status": status,
        },
    )


@app.get("/matches/{match_id}/start")
async def start_match(match_id: str):
    match = await db.matches.find_one({"_id": oid(match_id)})
    if not match:
        return HTMLResponse("Match not found", status_code=404)

    await db.matches.update_one({"_id": oid(match_id)}, {"$set": {"status": "live", "started_at": datetime.utcnow()}})
    return RedirectResponse(url=f"/matches/{match_id}/scoring", status_code=303)


@app.get("/matches/{match_id}/details", response_class=HTMLResponse)
async def match_details(request: Request, match_id: str):
    match = await db.matches.find_one({"_id": oid(match_id)})
    if not match:
        return HTMLResponse("Match not found", status_code=404)

    match = safe_str_id(match)
    team_map = await fetch_team_map({match["team1_id"], match["team2_id"]})
    tournament_map = await fetch_tournament_map({match["tournament_id"]})
    innings_list = await db.innings.find({"match_id": match_id}).sort("innings_number", 1).to_list(length=10)
    safe_str_ids(innings_list)

    return templates.TemplateResponse(
        request=request,
        name="match_details.html",
        context={
            "match": match,
            "team1": team_map.get(match["team1_id"]),
            "team2": team_map.get(match["team2_id"]),
            "tournament": tournament_map.get(match["tournament_id"]),
            "innings_list": innings_list,
        },
    )


@app.get("/match-history", response_class=HTMLResponse)
async def match_history(request: Request):
    matches = await db.matches.find({"status": "completed"}).sort("_id", -1).to_list(length=100)
    matches = await enrich_matches_basic(matches)
    return templates.TemplateResponse(request=request, name="match_history.html", context={"matches": matches})


# =========================================================
# Search
# =========================================================
@app.get("/search", response_class=HTMLResponse)
async def global_search(request: Request, q: str = "", type: str = ""):
    results = {"players": [], "teams": [], "tournaments": [], "matches": []}

    if q:
        regex_query = {"$regex": q, "$options": "i"}

        if type in ["", "player"]:
            players = await db.players.find({"name": regex_query}).limit(20).to_list(length=20)
            results["players"] = safe_str_ids(players)

        if type in ["", "team"]:
            teams = await db.teams.find({"name": regex_query}).limit(20).to_list(length=20)
            results["teams"] = safe_str_ids(teams)

        if type in ["", "tournament"]:
            tournaments = await db.tournaments.find({"name": regex_query}).limit(20).to_list(length=20)
            results["tournaments"] = safe_str_ids(tournaments)

        if type in ["", "match"]:
            matches = await db.matches.find({
                "$or": [
                    {"round_type": regex_query},
                    {"team1_name": regex_query},
                    {"team2_name": regex_query},
                    {"tournament_name": regex_query},
                ],
            }).limit(20).to_list(length=20)
            results["matches"] = safe_str_ids(matches)

    return templates.TemplateResponse(request=request, name="search_results.html", context={"q": q, "type": type, "results": results})


# =========================================================
# Rules
# =========================================================
@app.get("/official-rules", response_class=HTMLResponse)
async def official_rules(request: Request):
    return templates.TemplateResponse(request=request, name="official_rules.html", context={})


# =========================================================
# Scoring
# =========================================================
@app.get("/matches/{match_id}/scoring", response_class=HTMLResponse)
async def scoring_page(request: Request, match_id: str):
    match = await db.matches.find_one({"_id": oid(match_id)})
    if not match:
        return HTMLResponse("Match not found", status_code=404)

    team_map = await fetch_team_map({match["team1_id"], match["team2_id"]})
    tournament_map = await fetch_tournament_map({match["tournament_id"]})

    team1 = team_map.get(match["team1_id"])
    team2 = team_map.get(match["team2_id"])
    tournament = tournament_map.get(match["tournament_id"])

    if not team1 or not team2 or not tournament:
        return HTMLResponse("Related match data not found", status_code=404)

    match = safe_str_id(match)

    team1_players = await db.players.find({"team_id": match["team1_id"], "is_playing": True}).sort("name", 1).to_list(length=20)
    team2_players = await db.players.find({"team_id": match["team2_id"], "is_playing": True}).sort("name", 1).to_list(length=20)
    safe_str_ids(team1_players)
    safe_str_ids(team2_players)

    team1_playing_count = len(team1_players)
    team2_playing_count = len(team2_players)
    team1_finalized = team1_playing_count == PLAYING_XI_COUNT
    team2_finalized = team2_playing_count == PLAYING_XI_COUNT

    innings = await get_current_or_latest_innings(match_id)

    innings_data = None
    balls: List[Dict[str, Any]] = []
    current_over_balls: List[Dict[str, Any]] = []
    setup_locked = False
    striker = None
    non_striker = None
    current_bowler = None
    dismissed_batsman_ids: Set[str] = set()

    striker_stats = {"runs": 0, "balls": 0, "fours": 0, "sixes": 0}
    non_striker_stats = {"runs": 0, "balls": 0, "fours": 0, "sixes": 0}
    bowler_stats = {"overs": 0.0, "runs": 0, "wickets": 0, "economy": 0.0}
    c_rr = 0.0
    r_rr = None

    innings_list = await db.innings.find({"match_id": match_id}).sort("innings_number", 1).to_list(length=10)
    innings_scorecards = []
    for inn in innings_list:
        innings_scorecards.append(await build_innings_scorecard(inn))

    current_innings_number = 1
    batting_team_id = match.get("batting_team_id")
    bowling_team_id = match.get("bowling_team_id")

    if innings:
        innings = safe_str_id(innings)
        innings_data = innings
        current_innings_number = innings_data.get("innings_number", 1)
        batting_team_id = innings_data.get("batting_team_id", batting_team_id)
        bowling_team_id = innings_data.get("bowling_team_id", bowling_team_id)

    if batting_team_id == match["team1_id"]:
        batting_players = team1_players
        bowling_players = team2_players
    elif batting_team_id == match["team2_id"]:
        batting_players = team2_players
        bowling_players = team1_players
    else:
        batting_players = team1_players
        bowling_players = team2_players

    auto_open_action_modal = False
    auto_open_end_innings = False

    if innings_data:
        balls = await db.balls.find({"innings_id": innings_data["_id"]}).sort("_id", 1).to_list(length=1000)
        safe_str_ids(balls)

        setup_locked = len(balls) > 0 or bool(innings_data.get("striker_id")) or bool(innings_data.get("non_striker_id"))

        player_ids: Set[str] = set()
        if innings_data.get("striker_id"):
            player_ids.add(innings_data["striker_id"])
        if innings_data.get("non_striker_id"):
            player_ids.add(innings_data["non_striker_id"])
        if innings_data.get("current_bowler_id"):
            player_ids.add(innings_data["current_bowler_id"])

        for ball in balls:
            if ball.get("batsman_id"):
                player_ids.add(ball["batsman_id"])
            if ball.get("bowler_id"):
                player_ids.add(ball["bowler_id"])
            if ball.get("fielder_id"):
                player_ids.add(ball["fielder_id"])

            dismissed_player_id = ball.get("dismissed_player_id")
            wicket_type = ball.get("wicket_type")
            if ball.get("is_wicket") and dismissed_player_id and wicket_type != "retiredhurt":
                dismissed_batsman_ids.add(dismissed_player_id)

        player_map = await fetch_player_map(player_ids)

        striker = player_map.get(innings_data.get("striker_id"))
        non_striker = player_map.get(innings_data.get("non_striker_id"))
        current_bowler = player_map.get(innings_data.get("current_bowler_id"))

        current_over_number = int(innings_data.get("legal_balls", 0) / 6)
        for ball in balls:
            if ball.get("over_number") == current_over_number:
                ball["batsman_name"] = player_map.get(ball.get("batsman_id"), {}).get("name", "-")
                current_over_balls.append(ball)

        striker_id_val = innings_data.get("striker_id")
        non_striker_id_val = innings_data.get("non_striker_id")
        bowler_id_val = innings_data.get("current_bowler_id")

        bowler_legal_balls = 0
        legal_balls_total = innings_data.get("legal_balls", 0)

        for ball in balls:
            batsman_id = ball.get("batsman_id")
            bowler_id = ball.get("bowler_id")
            extra_type = ball.get("extras_type")

            bat_runs = ball.get("bat_runs", ball.get("runs", 0))
            wide_base = ball.get("wide_base", 0)
            wide_run_runs = ball.get("wide_run_runs", 0)

            if extra_type == "wide" and wide_base == 0 and wide_run_runs == 0:
                old_extras = ball.get("extras_runs", 0)
                if old_extras > 0:
                    wide_base = 1
                    wide_run_runs = max(0, old_extras - 1)

            wide_total = wide_base + wide_run_runs
            bowler_runs_this_ball = ball.get("bowler_runs_this_ball")
            if bowler_runs_this_ball is None:
                bowler_runs_this_ball = bat_runs + wide_total + (1 if extra_type == "noball" else 0)

            counts_as_legal_ball = get_counts_as_legal(extra_type, ball.get("counts_as_legal_ball"))

            if batsman_id == striker_id_val:
                striker_stats["runs"] += bat_runs
                if counts_as_legal_ball:
                    striker_stats["balls"] += 1
                if bat_runs == 4:
                    striker_stats["fours"] += 1
                if bat_runs == 6:
                    striker_stats["sixes"] += 1

            if batsman_id == non_striker_id_val:
                non_striker_stats["runs"] += bat_runs
                if counts_as_legal_ball:
                    non_striker_stats["balls"] += 1
                if bat_runs == 4:
                    non_striker_stats["fours"] += 1
                if bat_runs == 6:
                    non_striker_stats["sixes"] += 1

            if bowler_id == bowler_id_val:
                bowler_stats["runs"] += bowler_runs_this_ball
                if ball.get("is_wicket") and ball.get("bowler_wicket_credit"):
                    bowler_stats["wickets"] += 1
                if counts_as_legal_ball:
                    bowler_legal_balls += 1

        bowler_stats["overs"] = calculate_overs_from_balls(bowler_legal_balls)
        if bowler_legal_balls > 0:
            bowler_stats["economy"] = round(bowler_stats["runs"] / overs_to_decimal(bowler_legal_balls), 2)

        total_runs_val = innings_data.get("total_runs", 0)
        if legal_balls_total > 0:
            c_rr = round(total_runs_val / overs_to_decimal(legal_balls_total), 2)

        if current_innings_number == 2 and match.get("target"):
            balls_left = max(0, MAX_BALLS_PER_INNINGS - legal_balls_total)
            runs_needed = match["target"] - total_runs_val
            if balls_left > 0 and runs_needed > 0:
                r_rr = round((runs_needed * 6) / balls_left, 2)
            elif runs_needed <= 0:
                r_rr = 0.0

        if innings_data.get("innings_status") == "live" and (
            innings_data.get("wickets", 0) >= MAX_WICKETS or legal_balls_total >= MAX_BALLS_PER_INNINGS
        ):
            auto_open_action_modal = True
            auto_open_end_innings = True

    return templates.TemplateResponse(
        request=request,
        name="scoring.html",
        context={
            "match": match,
            "team1": team1,
            "team2": team2,
            "tournament": tournament,
            "team1_players": team1_players,
            "team2_players": team2_players,
            "batting_players": batting_players,
            "bowling_players": bowling_players,
            "team1_playing_count": team1_playing_count,
            "team2_playing_count": team2_playing_count,
            "team1_finalized": team1_finalized,
            "team2_finalized": team2_finalized,
            "innings": innings_data,
            "innings_list": innings_list,
            "innings_scorecards": innings_scorecards,
            "current_innings_number": current_innings_number,
            "balls": balls,
            "setup_locked": setup_locked,
            "striker": striker,
            "non_striker": non_striker,
            "current_bowler": current_bowler,
            "dismissed_batsman_ids": list(dismissed_batsman_ids),
            "striker_stats": striker_stats,
            "non_striker_stats": non_striker_stats,
            "bowler_stats": bowler_stats,
            "c_rr": c_rr,
            "r_rr": r_rr,
            "current_over_balls": current_over_balls,
            "auto_open_action_modal": auto_open_action_modal,
            "auto_open_end_innings": auto_open_end_innings,
        },
    )


@app.post("/matches/{match_id}/toss")
async def save_toss_setup(
    match_id: str,
    scorer_name: str = Form(...),
    toss_winner_id: str = Form(...),
    toss_decision: str = Form(...),
):
    match = await db.matches.find_one({"_id": oid(match_id)})
    if not match:
        return HTMLResponse("Match not found", status_code=404)

    if toss_winner_id not in [match["team1_id"], match["team2_id"]]:
        return HTMLResponse("Invalid toss winner", status_code=400)

    if toss_decision == "Bat":
        batting_team_id = toss_winner_id
        bowling_team_id = match["team2_id"] if toss_winner_id == match["team1_id"] else match["team1_id"]
    else:
        bowling_team_id = toss_winner_id
        batting_team_id = match["team2_id"] if toss_winner_id == match["team1_id"] else match["team1_id"]

    await db.matches.update_one(
        {"_id": oid(match_id)},
        {"$set": {
            "scorer_name": scorer_name,
            "toss_winner_id": toss_winner_id,
            "toss_decision": toss_decision,
            "batting_team_id": batting_team_id,
            "bowling_team_id": bowling_team_id,
            "status": "live",
        }},
    )
    return RedirectResponse(url=f"/matches/{match_id}/scoring", status_code=303)


@app.post("/matches/{match_id}/start-innings")
async def start_innings_setup(
    match_id: str,
    striker_id: str = Form(...),
    non_striker_id: str = Form(...),
    current_bowler_id: str = Form(...),
):
    match = await db.matches.find_one({"_id": oid(match_id)})
    if not match:
        return HTMLResponse("Match not found", status_code=404)

    innings = await get_current_or_latest_innings(match_id)

    if innings and innings.get("innings_status") in ["live", "pending"]:
        innings_number = innings.get("innings_number", 1)
        batting_team_id = innings.get("batting_team_id")
        bowling_team_id = innings.get("bowling_team_id")
    else:
        innings_number = 1
        batting_team_id = match.get("batting_team_id")
        bowling_team_id = match.get("bowling_team_id")

    if not batting_team_id or not bowling_team_id:
        return HTMLResponse("Toss/setup is incomplete", status_code=400)

    validation_error = await validate_match_player_roles(
        match, batting_team_id, bowling_team_id, striker_id, non_striker_id, current_bowler_id
    )
    if validation_error:
        return validation_error

    innings_doc = {
        "match_id": match_id,
        "innings_number": innings_number,
        "batting_team_id": batting_team_id,
        "bowling_team_id": bowling_team_id,
        "total_runs": innings.get("total_runs", 0) if innings else 0,
        "wickets": innings.get("wickets", 0) if innings else 0,
        "legal_balls": innings.get("legal_balls", 0) if innings else 0,
        "overs": innings.get("overs", 0.0) if innings else 0.0,
        "striker_id": striker_id,
        "non_striker_id": non_striker_id,
        "current_bowler_id": current_bowler_id,
        "opening_striker_id": innings.get("opening_striker_id") if innings else striker_id,
        "opening_non_striker_id": innings.get("opening_non_striker_id") if innings else non_striker_id,
        "opening_bowler_id": innings.get("opening_bowler_id") if innings else current_bowler_id,
        "innings_status": "live",
    }

    if not innings_doc["opening_striker_id"]:
        innings_doc["opening_striker_id"] = striker_id
    if not innings_doc["opening_non_striker_id"]:
        innings_doc["opening_non_striker_id"] = non_striker_id
    if not innings_doc["opening_bowler_id"]:
        innings_doc["opening_bowler_id"] = current_bowler_id

    if innings and innings.get("innings_status") in ["live", "pending"]:
        await db.innings.update_one({"_id": innings["_id"]}, {"$set": innings_doc})
    else:
        await db.innings.insert_one(innings_doc)

    return RedirectResponse(url=f"/matches/{match_id}/scoring", status_code=303)


@app.get("/teams/{team_id}/playing-check")
async def check_team_playing(team_id: str):
    count = await db.players.count_documents({"team_id": team_id, "is_playing": True})
    return {"team_id": team_id, "playing_count": count, "finalized": count == PLAYING_XI_COUNT}


@app.post("/matches/{match_id}/score/run")
async def score_run(match_id: str, runs: int = Form(...)):
    match = await db.matches.find_one({"_id": oid(match_id)})
    if not match:
        return HTMLResponse("Match not found", status_code=404)

    innings = await db.innings.find_one({"match_id": match_id, "innings_status": "live"})
    if not innings:
        return HTMLResponse("Live innings not found", status_code=404)

    innings_id = str(innings["_id"])
    legal_balls_before = innings.get("legal_balls", 0)
    over_number = int(legal_balls_before / 6)
    ball_number = (legal_balls_before % 6) + 1

    ball_doc = {
        "innings_id": innings_id,
        "match_id": match_id,
        "over_number": over_number,
        "ball_number": ball_number,
        "batsman_id": innings["striker_id"],
        "non_striker_id": innings["non_striker_id"],
        "bowler_id": innings["current_bowler_id"],
        "bat_runs": runs,
        "bye_runs": 0,
        "legbye_runs": 0,
        "wide_base": 0,
        "wide_run_runs": 0,
        "noball_base": 0,
        "noball_subtype": None,
        "runs": runs,
        "extras_type": None,
        "extras_runs": 0,
        "counts_as_legal_ball": True,
        "team_runs_this_ball": runs,
        "batsman_runs_this_ball": runs,
        "bowler_runs_this_ball": runs,
        "strike_run_component": runs,
        "is_wicket": False,
        "wicket_type": None,
        "dismissed_player_id": None,
        "new_batsman_id": None,
        "fielder_id": None,
        "bowler_wicket_credit": False,
        "ball_description": f"{runs} run" if runs == 1 else f"{runs} runs",
    }

    await db.balls.insert_one(ball_doc)

    legal_balls_after = legal_balls_before + 1
    striker_id = innings["striker_id"]
    non_striker_id = innings["non_striker_id"]

    if runs in [1, 3, 5]:
        striker_id, non_striker_id = non_striker_id, striker_id
    if legal_balls_after % 6 == 0:
        striker_id, non_striker_id = non_striker_id, striker_id

    innings_status = "completed" if legal_balls_after >= MAX_BALLS_PER_INNINGS else "live"

    await db.innings.update_one(
        {"_id": innings["_id"]},
        {"$set": {
            "total_runs": innings.get("total_runs", 0) + runs,
            "wickets": innings.get("wickets", 0),
            "legal_balls": legal_balls_after,
            "overs": calculate_overs_from_balls(legal_balls_after),
            "striker_id": striker_id,
            "non_striker_id": non_striker_id,
            "innings_status": innings_status,
        }},
    )

    if innings_status == "completed" and innings.get("innings_number", 1) == 1:
        await db.matches.update_one({"_id": oid(match_id)}, {"$set": {"target": innings.get("total_runs", 0) + runs + 1}})
        await create_second_innings_if_missing(match_id, innings)

    return RedirectResponse(url=f"/matches/{match_id}/scoring", status_code=303)


@app.post("/matches/{match_id}/score/extra-common")
async def score_extra_common(
    match_id: str,
    extra_type: str = Form(...),
    runs: int = Form(...),
    noball_subtype: str = Form(""),
):
    match = await db.matches.find_one({"_id": oid(match_id)})
    if not match:
        return HTMLResponse("Match not found", status_code=404)

    innings = await db.innings.find_one({"match_id": match_id, "innings_status": "live"})
    if not innings:
        return HTMLResponse("Live innings not found", status_code=404)

    innings_id = str(innings["_id"])
    striker_id = innings["striker_id"]
    non_striker_id = innings["non_striker_id"]
    bowler_id = innings["current_bowler_id"]
    legal_balls_before = innings.get("legal_balls", 0)

    over_number = int(legal_balls_before / 6)
    ball_number = (legal_balls_before % 6) + 1
    counts_as_legal = extra_type not in ["wide", "noball"]

    bat_runs = 0
    bye_runs = 0
    legbye_runs = 0
    wide_base = 0
    wide_run_runs = 0
    noball_base = 0
    noball_subtype_value = None
    team_runs_this_ball = 0
    batsman_runs_this_ball = 0
    bowler_runs_this_ball = 0
    strike_run_component = 0

    if extra_type == "wide":
        wide_base = 1
        wide_run_runs = runs
        team_runs_this_ball = 1 + runs
        bowler_runs_this_ball = 1 + runs
        strike_run_component = runs
        description = "WIDE" if runs == 0 else f"WIDE + {runs} run(s)"
    elif extra_type == "bye":
        bye_runs = runs
        team_runs_this_ball = runs
        strike_run_component = runs
        description = "BYE" if runs == 0 else f"BYE + {runs}"
    elif extra_type == "legbye":
        legbye_runs = runs
        team_runs_this_ball = runs
        strike_run_component = runs
        description = "LEG BYE" if runs == 0 else f"LEG BYE + {runs}"
    elif extra_type == "noball":
        noball_base = 1
        noball_subtype_value = noball_subtype
        if noball_subtype == "bat":
            bat_runs = runs
            team_runs_this_ball = 1 + runs
            batsman_runs_this_ball = runs
            bowler_runs_this_ball = 1 + runs
            strike_run_component = runs
            description = "NO BALL" if runs == 0 else f"NO BALL + BAT {runs}"
        elif noball_subtype == "bye":
            bye_runs = runs
            team_runs_this_ball = 1 + runs
            bowler_runs_this_ball = 1
            strike_run_component = runs
            description = "NO BALL" if runs == 0 else f"NO BALL + BYE {runs}"
        elif noball_subtype == "legbye":
            legbye_runs = runs
            team_runs_this_ball = 1 + runs
            bowler_runs_this_ball = 1
            strike_run_component = runs
            description = "NO BALL" if runs == 0 else f"NO BALL + LEG BYE {runs}"
        else:
            return HTMLResponse("No-ball subtype required", status_code=400)
    else:
        return HTMLResponse("Invalid extra type", status_code=400)

    ball_doc = {
        "innings_id": innings_id,
        "match_id": match_id,
        "over_number": over_number,
        "ball_number": ball_number,
        "batsman_id": striker_id,
        "non_striker_id": non_striker_id,
        "bowler_id": bowler_id,
        "bat_runs": bat_runs,
        "bye_runs": bye_runs,
        "legbye_runs": legbye_runs,
        "wide_base": wide_base,
        "wide_run_runs": wide_run_runs,
        "noball_base": noball_base,
        "noball_subtype": noball_subtype_value,
        "runs": batsman_runs_this_ball,
        "extras_type": extra_type,
        "extras_runs": team_runs_this_ball - batsman_runs_this_ball,
        "is_wicket": False,
        "wicket_type": None,
        "dismissed_player_id": None,
        "new_batsman_id": None,
        "fielder_id": None,
        "bowler_wicket_credit": False,
        "counts_as_legal_ball": counts_as_legal,
        "team_runs_this_ball": team_runs_this_ball,
        "batsman_runs_this_ball": batsman_runs_this_ball,
        "bowler_runs_this_ball": bowler_runs_this_ball,
        "strike_run_component": strike_run_component,
        "ball_description": description,
    }

    await db.balls.insert_one(ball_doc)

    legal_balls_after = legal_balls_before + (1 if counts_as_legal else 0)
    updated_striker_id = striker_id
    updated_non_striker_id = non_striker_id

    if strike_run_component in [1, 3, 5]:
        updated_striker_id, updated_non_striker_id = updated_non_striker_id, updated_striker_id
    if counts_as_legal and legal_balls_after % 6 == 0:
        updated_striker_id, updated_non_striker_id = updated_non_striker_id, updated_striker_id

    innings_status = "completed" if legal_balls_after >= MAX_BALLS_PER_INNINGS else "live"

    await db.innings.update_one(
        {"_id": innings["_id"]},
        {"$set": {
            "total_runs": innings.get("total_runs", 0) + team_runs_this_ball,
            "legal_balls": legal_balls_after,
            "overs": calculate_overs_from_balls(legal_balls_after),
            "striker_id": updated_striker_id,
            "non_striker_id": updated_non_striker_id,
            "innings_status": innings_status,
        }},
    )

    if innings_status == "completed" and innings.get("innings_number", 1) == 1:
        await db.matches.update_one({"_id": oid(match_id)}, {"$set": {"target": innings.get("total_runs", 0) + team_runs_this_ball + 1}})
        await create_second_innings_if_missing(match_id, innings)

    return RedirectResponse(url=f"/matches/{match_id}/scoring", status_code=303)


@app.post("/matches/{match_id}/score/wicket")
async def score_wicket(match_id: str, wicket_type: str = Form(...)):
    return await score_wicket_common(
        match_id=match_id,
        wicket_type=wicket_type,
        dismissed_side="striker",
        runs_completed=0,
        bowler_id="__current__",
        new_batsman_id=None,
        fielder_id="",
    )


@app.post("/matches/{match_id}/score/wicket-common")
async def score_wicket_common(
    match_id: str,
    wicket_type: str = Form(...),
    dismissed_side: str = Form(...),
    runs_completed: int = Form(0),
    bowler_id: str = Form(...),
    new_batsman_id: Optional[str] = Form(None),
    fielder_id: str = Form(""),
):
    match = await db.matches.find_one({"_id": oid(match_id)})
    if not match:
        return HTMLResponse("Match not found", status_code=404)

    innings = await db.innings.find_one({"match_id": match_id, "innings_status": "live"})
    if not innings:
        return HTMLResponse("Live innings not found", status_code=404)

    innings_id = str(innings["_id"])
    striker_id = innings.get("striker_id")
    non_striker_id = innings.get("non_striker_id")
    current_bowler_id = innings.get("current_bowler_id")

    if not striker_id or not non_striker_id or not current_bowler_id:
        return HTMLResponse("Innings setup incomplete", status_code=400)

    if bowler_id == "__current__":
        bowler_id = current_bowler_id

    if bowler_id != current_bowler_id:
        return HTMLResponse("Bowler mismatch", status_code=400)

    if dismissed_side not in ["striker", "non_striker"]:
        return HTMLResponse("Invalid dismissed_side", status_code=400)

    dismissed_player_id = striker_id if dismissed_side == "striker" else non_striker_id

    if new_batsman_id and new_batsman_id in [striker_id, non_striker_id]:
        return HTMLResponse("New batsman must be different from current batsmen", status_code=400)

    legal_balls_before = innings.get("legal_balls", 0)
    over_number = int(legal_balls_before / 6)
    ball_number = (legal_balls_before % 6) + 1

    count_as_wicket = wicket_type != "retiredhurt"
    bowler_gets_wicket = wicket_type in ["bowled", "caught", "stumped", "sixout"]

    description_map = {
        "bowled": "Bowled",
        "caught": "Caught",
        "runout": "Run Out",
        "stumped": "Stumped",
        "sixout": "Six-Out",
        "retiredhurt": "Retired Hurt",
    }

    runs_scored = runs_completed if wicket_type == "runout" else 0

    ball_doc = {
        "innings_id": innings_id,
        "match_id": match_id,
        "over_number": over_number,
        "ball_number": ball_number,
        "batsman_id": striker_id,
        "non_striker_id": non_striker_id,
        "bowler_id": bowler_id,
        "runs": runs_scored,
        "bat_runs": runs_scored,
        "extras_type": None,
        "extras_runs": 0,
        "counts_as_legal_ball": True,
        "team_runs_this_ball": runs_scored,
        "batsman_runs_this_ball": runs_scored,
        "bowler_runs_this_ball": runs_scored,
        "strike_run_component": runs_scored,
        "is_wicket": count_as_wicket,
        "wicket_type": wicket_type,
        "dismissed_player_id": dismissed_player_id,
        "new_batsman_id": new_batsman_id,
        "fielder_id": fielder_id if fielder_id else None,
        "ball_description": description_map.get(wicket_type, "Wicket"),
        "bowler_wicket_credit": bowler_gets_wicket,
    }

    await db.balls.insert_one(ball_doc)

    new_total_runs = innings.get("total_runs", 0) + runs_scored
    new_wickets = innings.get("wickets", 0) + (1 if count_as_wicket else 0)
    legal_balls_after = legal_balls_before + 1

    updated_striker_id = striker_id
    updated_non_striker_id = non_striker_id

    if dismissed_side == "striker":
        updated_striker_id = new_batsman_id if new_batsman_id else None
    else:
        updated_non_striker_id = new_batsman_id if new_batsman_id else None

    if runs_scored in [1, 3, 5]:
        updated_striker_id, updated_non_striker_id = updated_non_striker_id, updated_striker_id

    if legal_balls_after % 6 == 0:
        updated_striker_id, updated_non_striker_id = updated_non_striker_id, updated_striker_id

    innings_status = "live"
    if new_wickets >= MAX_WICKETS or legal_balls_after >= MAX_BALLS_PER_INNINGS:
        innings_status = "completed"

    await db.innings.update_one(
        {"_id": innings["_id"]},
        {"$set": {
            "total_runs": new_total_runs,
            "wickets": new_wickets,
            "legal_balls": legal_balls_after,
            "overs": calculate_overs_from_balls(legal_balls_after),
            "striker_id": updated_striker_id,
            "non_striker_id": updated_non_striker_id,
            "innings_status": innings_status,
        }},
    )

    if innings_status == "completed" and innings.get("innings_number", 1) == 1:
        await db.matches.update_one({"_id": oid(match_id)}, {"$set": {"target": new_total_runs + 1}})
        await create_second_innings_if_missing(match_id, innings)

    return RedirectResponse(url=f"/matches/{match_id}/scoring", status_code=303)


@app.post("/matches/{match_id}/action/end-over")
async def end_over_action(match_id: str, new_bowler_id: str = Form(...)):
    match = await db.matches.find_one({"_id": oid(match_id)})
    if not match:
        return HTMLResponse("Match not found", status_code=404)

    innings = await db.innings.find_one({"match_id": match_id, "innings_status": "live"})
    if not innings:
        return HTMLResponse("Live innings not found", status_code=404)

    current_bowler_id = innings.get("current_bowler_id")
    if current_bowler_id == new_bowler_id:
        return RedirectResponse(url=f"/matches/{match_id}/scoring", status_code=303)

    bowler = await db.players.find_one({"_id": oid(new_bowler_id), "team_id": innings["bowling_team_id"], "is_playing": True})
    if not bowler:
        return HTMLResponse("Invalid new bowler", status_code=400)

    await db.innings.update_one({"_id": innings["_id"]}, {"$set": {"current_bowler_id": new_bowler_id}})
    return RedirectResponse(url=f"/matches/{match_id}/scoring", status_code=303)


@app.post("/matches/{match_id}/action/undo")
async def undo_action(match_id: str):
    innings = await db.innings.find_one(
        {"match_id": match_id, "innings_status": {"$in": ["live", "pending", "completed"]}},
        sort=[("innings_number", -1)],
    )
    if not innings:
        return HTMLResponse("Innings not found", status_code=404)

    innings_id = str(innings["_id"])
    last_ball = await db.balls.find({"innings_id": innings_id}).sort("_id", -1).to_list(length=1)
    if not last_ball:
        return RedirectResponse(url=f"/matches/{match_id}/scoring", status_code=303)

    await db.balls.delete_one({"_id": last_ball[0]["_id"]})

    rebuilt = await rebuild_innings_state(innings)
    await db.innings.update_one({"_id": innings["_id"]}, {"$set": rebuilt})
    await sync_match_state(match_id)

    return RedirectResponse(url=f"/matches/{match_id}/scoring", status_code=303)


@app.post("/matches/{match_id}/action/complete-match")
async def complete_match_action(match_id: str, winner_result: str = Form(...)):
    match = await db.matches.find_one({"_id": oid(match_id)})
    if not match:
        return HTMLResponse("Match not found", status_code=404)

    team_map = await fetch_team_map({match["team1_id"], match["team2_id"]})
    team1 = team_map.get(match["team1_id"])
    team2 = team_map.get(match["team2_id"])

    innings_list = await db.innings.find({"match_id": match_id}).sort("innings_number", 1).to_list(length=10)
    team1_score = None
    team2_score = None

    for inn in innings_list:
        if inn.get("batting_team_id") == match["team1_id"]:
            team1_score = f'{inn.get("total_runs", 0)}/{inn.get("wickets", 0)}'
        elif inn.get("batting_team_id") == match["team2_id"]:
            team2_score = f'{inn.get("total_runs", 0)}/{inn.get("wickets", 0)}'

    winner_team_id = None
    result_text = ""
    match_result = winner_result

    if winner_result == "team1":
        winner_team_id = match["team1_id"]
        result_text = f'{team1["name"]} won the match'
    elif winner_result == "team2":
        winner_team_id = match["team2_id"]
        result_text = f'{team2["name"]} won the match'
    elif winner_result == "tied":
        result_text = "Match tied"
    elif winner_result == "abandoned":
        result_text = "Match abandoned"
    else:
        return HTMLResponse("Invalid winner result", status_code=400)

    if team1_score or team2_score:
        result_text += f' | {team1["name"]}: {team1_score or "-"} | {team2["name"]}: {team2_score or "-"}'

    await db.matches.update_one(
        {"_id": oid(match_id)},
        {"$set": {
            "status": "completed",
            "winner_team_id": winner_team_id,
            "match_result": match_result,
            "result_text": result_text,
        }},
    )

    await db.innings.update_many({"match_id": match_id, "innings_status": "live"}, {"$set": {"innings_status": "completed"}})
    return RedirectResponse(url="/match-history", status_code=303)


@app.post("/matches/{match_id}/action/swap-strike")
async def swap_strike_action(match_id: str):
    innings = await db.innings.find_one({"match_id": match_id, "innings_status": "live"})
    if not innings:
        return HTMLResponse("Live innings not found", status_code=404)

    striker_id = innings.get("striker_id")
    non_striker_id = innings.get("non_striker_id")

    if not striker_id or not non_striker_id:
        return RedirectResponse(url=f"/matches/{match_id}/scoring", status_code=303)

    await db.innings.update_one(
        {"_id": innings["_id"]},
        {"$set": {"striker_id": non_striker_id, "non_striker_id": striker_id}},
    )
    return RedirectResponse(url=f"/matches/{match_id}/scoring", status_code=303)


@app.post("/matches/{match_id}/action/end-innings")
async def end_innings_action(match_id: str):
    match = await db.matches.find_one({"_id": oid(match_id)})
    if not match:
        return HTMLResponse("Match not found", status_code=404)

    innings = await db.innings.find_one({"match_id": match_id, "innings_status": "live"})
    if not innings:
        return HTMLResponse("Live innings not found", status_code=404)

    innings_number = innings.get("innings_number", 1)
    await db.innings.update_one({"_id": innings["_id"]}, {"$set": {"innings_status": "completed"}})

    if innings_number == 1:
        target = innings.get("total_runs", 0) + 1
        await db.matches.update_one({"_id": oid(match_id)}, {"$set": {"target": target}})
        await create_second_innings_if_missing(match_id, innings)

    return RedirectResponse(url=f"/matches/{match_id}/scoring", status_code=303)


@app.get("/players/{player_id}/stats-view", response_class=HTMLResponse)
async def player_stats_view(request: Request, player_id: str):
    player = await db.players.find_one({"_id": oid(player_id)})
    if not player:
        return HTMLResponse("Player not found", status_code=404)

    player = safe_str_id(player)
    team = await db.teams.find_one({"_id": oid(player["team_id"])}) if player.get("team_id") else None
    team = safe_str_id(team) if team else None
    tournament = None
    if team and team.get("tournament_id"):
        tournament = await db.tournaments.find_one({"_id": oid(team["tournament_id"])})
        tournament = safe_str_id(tournament) if tournament else None

    batting_balls = await db.balls.find({"batsman_id": player_id}).to_list(length=5000)
    bowling_balls = await db.balls.find({"bowler_id": player_id}).to_list(length=5000)
    fielding_balls = await db.balls.find({"fielder_id": player_id}).to_list(length=5000)

    batting_runs = 0
    balls_faced = 0
    fours = 0
    sixes = 0
    dismissals = 0

    for ball in batting_balls:
        bat_runs = ball.get("bat_runs", ball.get("runs", 0))
        extra_type = ball.get("extras_type")
        counts_as_legal = get_counts_as_legal(extra_type, ball.get("counts_as_legal_ball"))

        batting_runs += bat_runs
        if counts_as_legal:
            balls_faced += 1
        if bat_runs == 4:
            fours += 1
        if bat_runs == 6:
            sixes += 1
        if ball.get("is_wicket") and ball.get("dismissed_player_id") == player_id and ball.get("wicket_type") != "retiredhurt":
            dismissals += 1

    strike_rate = round((batting_runs / balls_faced) * 100, 2) if balls_faced > 0 else 0.0

    runs_conceded = 0
    bowling_wickets = 0
    legal_balls = 0

    for ball in bowling_balls:
        extra_type = ball.get("extras_type")
        counts_as_legal = get_counts_as_legal(extra_type, ball.get("counts_as_legal_ball"))

        bowler_runs_this_ball = ball.get("bowler_runs_this_ball")
        if bowler_runs_this_ball is None:
            bowler_runs_this_ball = ball.get("runs", 0) + ball.get("extras_runs", 0)

        runs_conceded += bowler_runs_this_ball
        if counts_as_legal:
            legal_balls += 1
        if ball.get("is_wicket") and ball.get("bowler_wicket_credit"):
            bowling_wickets += 1

    overs_bowled = calculate_overs_from_balls(legal_balls)
    economy = round(runs_conceded / overs_to_decimal(legal_balls), 2) if legal_balls > 0 else 0.0

    catches = 0
    runouts = 0
    stumpings = 0
    for ball in fielding_balls:
        wicket_type = ball.get("wicket_type")
        if wicket_type == "caught":
            catches += 1
        elif wicket_type == "runout":
            runouts += 1
        elif wicket_type == "stumped":
            stumpings += 1

    return templates.TemplateResponse(
        request=request,
        name="player_stats_view.html",
        context={
            "player": player,
            "team": team,
            "tournament": tournament,
            "batting": {
                "runs": batting_runs,
                "balls": balls_faced,
                "dismissals": dismissals,
                "fours": fours,
                "sixes": sixes,
                "strike_rate": strike_rate,
            },
            "bowling": {
                "runs_conceded": runs_conceded,
                "wickets": bowling_wickets,
                "overs": overs_bowled,
                "economy": economy,
            },
            "fielding": {
                "catches": catches,
                "runouts": runouts,
                "stumpings": stumpings,
            },
        },
    )
