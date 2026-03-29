"""
seed_db.py — SteamIQ Table Initialiser
Render build command:
  pip install -r requirements.txt && python seed_db.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from app import app
from models import db

def seed():
    with app.app_context():
        print("Creating database tables...")
        db.create_all()
        print("✅ Tables ready.")

if __name__ == "__main__":
    seed()