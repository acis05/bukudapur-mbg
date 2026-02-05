import argparse
from datetime import datetime, timedelta
import secrets

from bukudapur_mbg import create_app, db
from bukudapur_mbg.models import AccessCode

def now_utc():
    return datetime.utcnow()

def fmt_dt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else "-"

def list_codes(limit=50):
    rows = AccessCode.query.order_by(AccessCode.id.desc()).limit(limit).all()
    print(f"--- Last {len(rows)} access codes ---")
    for a in rows:
        print(
            f"{a.code} | {a.status} | dapur={a.dapur_name or '-'} | "
            f"start={fmt_dt(a.start_at)} | exp={fmt_dt(a.expires_at)}"
        )

def create_code(dapur_name, days, status="active"):
    code = "BDMBG-" + secrets.token_hex(4).upper()
    start_at = now_utc()
    expires_at = start_at + timedelta(days=days)

    acc = AccessCode(
        code=code,
        dapur_name=dapur_name or None,
        status=status,
        start_at=start_at,
        expires_at=expires_at,
    )
    db.session.add(acc)
    db.session.commit()

    print("✅ Created:", code)
    print("   dapur   :", dapur_name)
    print("   status  :", status)
    print("   expires :", fmt_dt(expires_at))

def extend_code(code, days):
    acc = AccessCode.query.filter_by(code=code.strip().upper()).first()
    if not acc:
        print("❌ Code not found:", code)
        return

    base = acc.expires_at if acc.expires_at and acc.expires_at > now_utc() else now_utc()
    acc.expires_at = base + timedelta(days=days)
    acc.status = "active"
    if not acc.start_at:
        acc.start_at = now_utc()

    db.session.commit()
    print("✅ Extended:", acc.code)
    print("   new exp :", fmt_dt(acc.expires_at))

def expire_code(code):
    acc = AccessCode.query.filter_by(code=code.strip().upper()).first()
    if not acc:
        print("❌ Code not found:", code)
        return
    acc.status = "expired"
    acc.expires_at = now_utc()
    db.session.commit()
    print("✅ Expired:", acc.code)

def main():
    parser = argparse.ArgumentParser("Manage BukuDapur MBG access codes")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_list = sub.add_parser("list")
    p_list.add_argument("--limit", type=int, default=50)

    p_create = sub.add_parser("create")
    p_create.add_argument("--name", required=True)
    p_create.add_argument("--days", type=int, required=True)
    p_create.add_argument("--status", default="active")

    p_extend = sub.add_parser("extend")
    p_extend.add_argument("--code", required=True)
    p_extend.add_argument("--days", type=int, required=True)

    p_exp = sub.add_parser("expire")
    p_exp.add_argument("--code", required=True)

    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        if args.cmd == "list":
            list_codes(args.limit)
        elif args.cmd == "create":
            create_code(args.name, args.days, args.status)
        elif args.cmd == "extend":
            extend_code(args.code, args.days)
        elif args.cmd == "expire":
            expire_code(args.code)

if __name__ == "__main__":
    main()
