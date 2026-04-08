import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = "mongodb+srv://Vedsu:CVxB6F2N700cQ0qu@cluster0.thbmwqi.mongodb.net/cricket_db?retryWrites=true&w=majority"
client = AsyncIOMotorClient(MONGO_URL)
db = client.cricket_db
