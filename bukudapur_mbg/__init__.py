import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from dotenv import load_dotenv

db = SQLAlchemy()
migrate = Migrate()


def _fix_database_url(url: str) -> str:
    """
    Railway sering memberi DATABASE_URL 'postgres://...'
    SQLAlchemy (terutama versi baru) lebih aman pakai 'postgresql://...'
    """
    if not url:
        return url
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


def create_app():
    load_dotenv()

    app = Flask(__name__)

    # Secret
    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret")

    # Database (support Railway)
    db_url = os.getenv("DATABASE_URL", "sqlite:///bukudapur.db")
    app.config["SQLALCHEMY_DATABASE_URI"] = _fix_database_url(db_url)
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    # App settings
    app.config["ADMIN_PIN"] = os.getenv("ADMIN_PIN", "123456")

    # Init extensions
    db.init_app(app)
    migrate.init_app(app, db)

    # Blueprint
    from .routes import bp
    app.register_blueprint(bp)

    return app
