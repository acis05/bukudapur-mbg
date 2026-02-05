import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from dotenv import load_dotenv

db = SQLAlchemy()
migrate = Migrate()

def create_app():
    # Load .env hanya untuk lokal (Railway pakai Variables, ini tetap aman)
    load_dotenv()

    app = Flask(__name__)

    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret")
    app.config["ADMIN_PIN"] = os.getenv("ADMIN_PIN", "123456")
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    # === DATABASE: Railway/Postgres or local SQLite ===
    db_url = os.getenv("DATABASE_URL")

    if db_url:
        # Railway / Heroku kadang pakai "postgres://", SQLAlchemy butuh "postgresql://"
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        app.config["SQLALCHEMY_DATABASE_URI"] = db_url
    else:
        app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///bukudapur.db"

    db.init_app(app)
    migrate.init_app(app, db)

    from .routes import bp
    app.register_blueprint(bp)

    return app
