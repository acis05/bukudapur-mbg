from __future__ import annotations

from datetime import datetime, timedelta, date
import secrets
import tempfile
from io import BytesIO

from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    session,
    flash,
    current_app,
    send_file,
)

from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side

from . import db
from .models import (
    AccessCode,
    Account,
    Supplier,
    Item,
    CashTransaction,
    JournalEntry,
    JournalLine,
    # Sales
    SalesInvoice,
    SalesInvoiceLine,
    ARPayment,
    # Purchase/AP
    Purchase,
    PurchaseItem,
    APayment,
    # Stock usage
    StockUsage,
)

from .pdf_utils import (
    pdf_doc, header_block, table_2col, table_3col, table_block,
    fmt_idr, footer_canvas, section_title, subsection_title
)

# =========================
# Blueprint
# =========================
bp = Blueprint("main", __name__)

# =========================
# Session Keys
# =========================
SESSION_KEY = "access_code"
ADMIN_SESSION_KEY = "admin_logged_in"


# ============================================================
# Helper: Admin
# ============================================================
def _admin_logged_in() -> bool:
    return bool(session.get(ADMIN_SESSION_KEY))


def _require_admin():
    if not _admin_logged_in():
        flash("Silakan login admin dulu.", "error")
        return redirect(url_for("main.admin_login"))
    return None


def _generate_code() -> str:
    return "BDMBG-" + secrets.token_hex(4).upper()


# ============================================================
# Helper: Access / Trial
# ============================================================
def _get_active_access():
    code = session.get(SESSION_KEY)
    if not code:
        return None

    acc = AccessCode.query.filter_by(code=code).first()
    if not acc:
        return None

    changed = acc.mark_expired_if_needed()
    if changed:
        db.session.commit()

    if acc.status == "expired":
        return None

    return acc


def _require_access():
    return _get_active_access()


# ============================================================
# Helper: Date parsing + range
# ============================================================
def _parse_date(date_str: str) -> datetime:
    # input HTML date: YYYY-MM-DD
    return datetime.strptime(date_str, "%Y-%m-%d")


def _parse_ymd(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _get_date_range_from_request(default_start_of_month: bool = True):
    """
    Querystring:
      ?from=YYYY-MM-DD&to=YYYY-MM-DD
    Default:
      to = today (UTC date)
      from = awal bulan jika default_start_of_month True, else 30 hari terakhir
    """
    dfrom = _parse_ymd(request.args.get("from"))
    dto = _parse_ymd(request.args.get("to"))

    today = datetime.utcnow().date()
    if dto is None:
        dto = today

    if dfrom is None:
        if default_start_of_month:
            dfrom = dto.replace(day=1)
        else:
            dfrom = dto - timedelta(days=30)

    return dfrom, dto


def _get_date_range_args():
    """
    Ambil query string:
      from / to  (format YYYY-MM-DD)
    Return: (from_dt, to_dt_exclusive, from_str, to_str)
    """
    from_str = (request.args.get("from") or request.args.get("from_date") or "").strip()
    to_str = (request.args.get("to") or request.args.get("to_date") or "").strip()

    from_dt = _parse_date(from_str) if from_str else None
    to_dt_excl = (_parse_date(to_str) + timedelta(days=1)) if to_str else None

    return from_dt, to_dt_excl, from_str, to_str


# ============================================================
# Helper: JournalLine -> JournalEntry FK (biar robust)
# ============================================================
def _jl_entry_fk():
    """
    Beberapa project pakai JournalLine.entry_id, sebagian journal_entry_id.
    Kita detect otomatis.
    """
    if hasattr(JournalLine, "entry_id"):
        return JournalLine.entry_id
    if hasattr(JournalLine, "journal_entry_id"):
        return JournalLine.journal_entry_id
    raise AttributeError("JournalLine tidak punya entry_id / journal_entry_id")


def _jl_base_query(from_dt=None, to_dt_excl=None):
    """
    Base query JournalLine yang JOIN ke JournalEntry (biar bisa filter/order by tanggal).
    FIX: pakai FK yang robust, bukan asumsi entry_id.
    """
    fk = _jl_entry_fk()
    q = JournalLine.query.join(JournalEntry, fk == JournalEntry.id)

    if from_dt:
        q = q.filter(JournalEntry.date >= from_dt)
    if to_dt_excl:
        q = q.filter(JournalEntry.date < to_dt_excl)

    return q


# ============================================================
# Helper: account balance (all time / optional date range by string)
# ============================================================
def _account_balance(code: str, from_str: str | None = None, to_str: str | None = None):
    fk = _jl_entry_fk()
    q = (
        JournalLine.query
        .join(JournalEntry, fk == JournalEntry.id)
        .filter(JournalLine.account_code == code)
    )

    if from_str:
        q = q.filter(JournalEntry.date >= _parse_date(from_str))
    if to_str:
        q = q.filter(JournalEntry.date < (_parse_date(to_str) + timedelta(days=1)))

    debit = db.session.query(db.func.sum(JournalLine.debit)).select_from(q.subquery()).scalar() or 0
    credit = db.session.query(db.func.sum(JournalLine.credit)).select_from(q.subquery()).scalar() or 0
    return float(debit) - float(credit)


# ============================================================
# Helper: account balance (BY DATE RANGE)  ✅ INI YANG TADI RUSAK
# ============================================================
from datetime import datetime, timedelta, date

def _account_balance_range(code: str, from_dt=None, to_dt=None):
    """
    Balance debit-credit untuk akun pada rentang tanggal.
    Bisa menerima:
      - from_dt/to_dt sebagai date (inclusive)
      - from_dt sebagai datetime
      - to_dt sebagai datetime (inclusive -> akan dibuat exclusive otomatis)
    """
    fk = _jl_entry_fk()

    # normalize from_dt
    if isinstance(from_dt, date) and not isinstance(from_dt, datetime):
        from_dt = datetime.combine(from_dt, datetime.min.time())

    # normalize to_dt:
    # - kalau date: inclusive end date -> jadikan exclusive +1 hari
    # - kalau datetime: kita anggap inclusive -> jadikan exclusive +1 detik/hari? (pakai +1 hari biar aman)
    to_dt_excl = None
    if to_dt is not None:
        if isinstance(to_dt, date) and not isinstance(to_dt, datetime):
            to_dt_excl = datetime.combine(to_dt, datetime.min.time()) + timedelta(days=1)
        else:
            # kalau datetime, tetap jadikan exclusive dengan +1 hari (anggap laporan harian)
            to_dt_excl = to_dt + timedelta(days=1)

    q = db.session.query(
        db.func.coalesce(db.func.sum(JournalLine.debit), 0.0).label("debit"),
        db.func.coalesce(db.func.sum(JournalLine.credit), 0.0).label("credit"),
    ).join(JournalEntry, fk == JournalEntry.id).filter(JournalLine.account_code == code)

    if from_dt:
        q = q.filter(JournalEntry.date >= from_dt)
    if to_dt_excl:
        q = q.filter(JournalEntry.date < to_dt_excl)

    row = q.first()
    debit = float(row.debit or 0.0)
    credit = float(row.credit or 0.0)
    return debit - credit


# ============================================================
# Helper: Jurnal otomatis
# ============================================================
def _create_journal_for_cash(tx: CashTransaction) -> JournalEntry:
    entry = JournalEntry(date=tx.date, memo=tx.memo, source="cash", source_id=tx.id)

    if tx.direction == "in":
        entry.lines.append(
            JournalLine(
                account_code=tx.cash_account_code,
                account_name=tx.cash_account_name,
                debit=tx.amount,
                credit=0,
            )
        )
        entry.lines.append(
            JournalLine(
                account_code=tx.counter_account_code,
                account_name=tx.counter_account_name,
                debit=0,
                credit=tx.amount,
            )
        )
    else:
        entry.lines.append(
            JournalLine(
                account_code=tx.counter_account_code,
                account_name=tx.counter_account_name,
                debit=tx.amount,
                credit=0,
            )
        )
        entry.lines.append(
            JournalLine(
                account_code=tx.cash_account_code,
                account_name=tx.cash_account_name,
                debit=0,
                credit=tx.amount,
            )
        )

    db.session.add(entry)
    db.session.flush()
    return entry


def _create_journal_for_purchase(purchase: Purchase) -> JournalEntry:
    """
    Pembelian hutang:
    Debit Persediaan (10051)
    Kredit Hutang Usaha (20011)
    """
    entry = JournalEntry(date=purchase.date, memo=purchase.memo, source="purchase", source_id=purchase.id)
    amount = float(purchase.total_amount or 0)

    inventory_acc = Account.query.filter_by(code="10051").first()
    ap_acc = Account.query.filter_by(code="20011").first()
    if not inventory_acc or not ap_acc:
        raise Exception("Akun Persediaan (10051) atau Hutang Usaha (20011) belum ada.")

    entry.lines.append(
        JournalLine(
            account_code=inventory_acc.code,
            account_name=inventory_acc.name,
            debit=amount,
            credit=0,
        )
    )
    entry.lines.append(
        JournalLine(
            account_code=ap_acc.code,
            account_name=ap_acc.name,
            debit=0,
            credit=amount,
        )
    )

    db.session.add(entry)
    db.session.flush()
    return entry


def _create_journal_for_ap_payment(payment: APayment) -> JournalEntry:
    """
    Bayar hutang:
    Debit Hutang Usaha (20011)
    Kredit Kas/Bank (dipilih)
    """
    entry = JournalEntry(date=payment.date, memo=payment.memo, source="ap_payment", source_id=payment.id)

    ap_acc = Account.query.filter_by(code="20011").first()
    cash_acc = Account.query.filter_by(code=payment.cash_account_code).first()
    if not ap_acc or not cash_acc:
        raise Exception("Akun Hutang Usaha atau Kas/Bank tidak ditemukan.")

    entry.lines.append(
        JournalLine(
            account_code=ap_acc.code,
            account_name=ap_acc.name,
            debit=float(payment.amount or 0),
            credit=0,
        )
    )
    entry.lines.append(
        JournalLine(
            account_code=cash_acc.code,
            account_name=cash_acc.name,
            debit=0,
            credit=float(payment.amount or 0),
        )
    )

    db.session.add(entry)
    db.session.flush()
    return entry


def _create_journal_for_stock_usage(u: StockUsage) -> JournalEntry:
    """
    Pemakaian stok:
    Debit HPP (dipilih)
    Kredit Persediaan (10051)
    """
    inv_acc = Account.query.filter_by(code="10051").first()
    hpp_acc = Account.query.filter_by(code=u.hpp_account_code).first()
    if not inv_acc or not hpp_acc:
        raise Exception("Akun Persediaan (10051) atau akun HPP tidak ditemukan.")

    entry = JournalEntry(date=u.date, memo=u.memo, source="stock_usage", source_id=u.id)

    entry.lines.append(
        JournalLine(
            account_code=hpp_acc.code,
            account_name=hpp_acc.name,
            debit=float(u.total_cost or 0),
            credit=0,
        )
    )
    entry.lines.append(
        JournalLine(
            account_code=inv_acc.code,
            account_name=inv_acc.name,
            debit=0,
            credit=float(u.total_cost or 0),
        )
    )

    db.session.add(entry)
    db.session.flush()
    return entry


def _next_invoice_no(prefix="INV"):
    today = datetime.utcnow().strftime("%Y%m%d")
    base = f"{prefix}-{today}-"
    last = (
        SalesInvoice.query.filter(SalesInvoice.invoice_no.like(base + "%"))
        .order_by(SalesInvoice.id.desc())
        .first()
    )
    if not last:
        return base + "001"
    try:
        seq = int(last.invoice_no.split("-")[-1]) + 1
    except Exception:
        seq = 1
    return base + f"{seq:03d}"


def _create_journal_for_invoice(inv: SalesInvoice) -> JournalEntry:
    entry = JournalEntry(
        date=inv.date,
        memo=f"Invoice {inv.invoice_no} - {inv.customer_name}",
        source="sales_invoice",
        source_id=inv.id,
    )

    # Debit Piutang
    entry.lines.append(
        JournalLine(
            account_code=inv.ar_account_code,
            account_name=inv.ar_account_name,
            debit=float(inv.total_amount or 0),
            credit=0,
        )
    )

    # Kredit Penjualan
    entry.lines.append(
        JournalLine(
            account_code=inv.revenue_account_code,
            account_name=inv.revenue_account_name,
            debit=0,
            credit=float(inv.total_amount or 0),
        )
    )

    db.session.add(entry)
    db.session.flush()
    return entry


def _create_journal_for_ar_payment(p: ARPayment, inv: SalesInvoice) -> JournalEntry:
    entry = JournalEntry(
        date=p.date,
        memo=f"Pelunasan {inv.invoice_no} - {inv.customer_name}",
        source="ar_payment",
        source_id=p.id,
    )

    entry.lines.append(
        JournalLine(
            account_code=p.cash_account_code,
            account_name=p.cash_account_name,
            debit=float(p.amount or 0),
            credit=0,
        )
    )
    entry.lines.append(
        JournalLine(
            account_code=inv.ar_account_code,
            account_name=inv.ar_account_name,
            debit=0,
            credit=float(p.amount or 0),
        )
    )

    db.session.add(entry)
    db.session.flush()
    return entry


def _arpay_memo(customer: str | None, note: str | None) -> str:
    cust = (customer or "").strip()
    note = (note or "").strip()
    if cust and note:
        return f"[AR] {cust} - {note}"
    if cust:
        return f"[AR] {cust}"
    if note:
        return f"[AR] {note}"
    return "[AR]"


# ============================================================
# Admin Routes
# ============================================================
@bp.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        pin = (request.form.get("pin") or "").strip()
        if pin == current_app.config.get("ADMIN_PIN"):
            session[ADMIN_SESSION_KEY] = True
            flash("Login admin berhasil.", "success")
            return redirect(url_for("main.admin_codes"))
        flash("PIN salah.", "error")
        return redirect(url_for("main.admin_login"))

    return render_template("admin_login.html")


@bp.post("/admin/logout")
def admin_logout():
    session.pop(ADMIN_SESSION_KEY, None)
    flash("Logout admin.", "success")
    return redirect(url_for("main.admin_login"))


@bp.get("/admin/codes")
def admin_codes():
    guard = _require_admin()
    if guard:
        return guard

    codes = AccessCode.query.order_by(AccessCode.id.desc()).limit(200).all()
    return render_template("admin_codes.html", codes=codes)


@bp.post("/admin/codes/create")
def admin_create_code():
    guard = _require_admin()
    if guard:
        return guard

    dapur_name = (request.form.get("dapur_name") or "").strip()
    status = (request.form.get("status") or "active").strip()
    days_str = (request.form.get("days") or "30").strip()

    try:
        days = int(days_str)
        if days <= 0:
            raise ValueError()
    except ValueError:
        flash("Durasi hari harus angka > 0.", "error")
        return redirect(url_for("main.admin_codes"))

    code = _generate_code()
    start_at = datetime.utcnow()
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

    flash(f"Kode dibuat: {code} (exp: {expires_at})", "success")
    return redirect(url_for("main.admin_codes"))


@bp.post("/admin/codes/extend")
def admin_extend_code():
    guard = _require_admin()
    if guard:
        return guard

    code = (request.form.get("code") or "").strip().upper()
    days_str = (request.form.get("days") or "").strip()

    try:
        days = int(days_str)
        if days <= 0:
            raise ValueError()
    except ValueError:
        flash("Hari perpanjangan harus angka > 0.", "error")
        return redirect(url_for("main.admin_codes"))

    acc = AccessCode.query.filter_by(code=code).first()
    if not acc:
        flash("Kode tidak ditemukan.", "error")
        return redirect(url_for("main.admin_codes"))

    now = datetime.utcnow()
    base = acc.expires_at if (acc.expires_at and acc.expires_at > now) else now
    acc.expires_at = base + timedelta(days=days)
    acc.status = "active"
    if not acc.start_at:
        acc.start_at = now

    db.session.commit()
    flash(f"Kode {acc.code} diperpanjang +{days} hari. Exp: {acc.expires_at}", "success")
    return redirect(url_for("main.admin_codes"))


@bp.post("/admin/codes/expire")
def admin_expire_code():
    guard = _require_admin()
    if guard:
        return guard

    code = (request.form.get("code") or "").strip().upper()
    acc = AccessCode.query.filter_by(code=code).first()
    if not acc:
        flash("Kode tidak ditemukan.", "error")
        return redirect(url_for("main.admin_codes"))

    acc.status = "expired"
    acc.expires_at = datetime.utcnow()
    db.session.commit()

    flash(f"Kode {acc.code} di-expire.", "success")
    return redirect(url_for("main.admin_codes"))


# ============================================================
# Home/Auth
# ============================================================
@bp.get("/")
def home():
    acc = _get_active_access()
    if not acc:
        return redirect(url_for("main.enter_code"))
    return redirect(url_for("main.dashboard"))


@bp.route("/enter", methods=["GET", "POST"])
def enter_code():
    if request.method == "POST":
        code = (request.form.get("code") or "").strip().upper()
        if not code:
            flash("Masukkan kode akses.", "error")
            return redirect(url_for("main.enter_code"))

        acc = AccessCode.query.filter_by(code=code).first()
        if not acc:
            flash("Kode tidak ditemukan.", "error")
            return redirect(url_for("main.enter_code"))

        if acc.mark_expired_if_needed():
            db.session.commit()

        if acc.status == "expired":
            session[SESSION_KEY] = acc.code
            return redirect(url_for("main.expired"))

        session[SESSION_KEY] = acc.code
        flash("Akses berhasil.", "success")
        return redirect(url_for("main.dashboard"))

    return render_template("enter_code.html")


@bp.post("/trial")
def create_trial():
    dapur_name = (request.form.get("dapur_name") or "").strip()

    code = _generate_code()
    start_at = datetime.utcnow()
    expires_at = start_at + timedelta(days=3)

    acc = AccessCode(
        code=code,
        dapur_name=dapur_name or None,
        status="trial",
        start_at=start_at,
        expires_at=expires_at,
    )
    db.session.add(acc)
    db.session.commit()

    session[SESSION_KEY] = code
    flash(f"Trial dibuat. Kode akses kamu: {code}", "success")
    return redirect(url_for("main.dashboard"))


from datetime import datetime, date

@bp.get("/dashboard")
def dashboard():
    acc = _get_active_access()
    if not acc:
        if session.get(SESSION_KEY):
            return redirect(url_for("main.expired"))
        return redirect(url_for("main.enter_code"))

    remaining = acc.expires_at - datetime.utcnow()
    remaining_hours = max(0, int(remaining.total_seconds() // 3600))

    # ===== ALL TIME (awal sekali s.d hari ini) =====
    def _dmin():
        return datetime(2000, 1, 1)  # aman untuk "sejak awal"

    def _dmax():
        # akhir hari ini (supaya transaksi hari ini ikut)
        now = datetime.utcnow()
        return datetime(now.year, now.month, now.day, 23, 59, 59)

    dfrom = _dmin()
    dto = _dmax()

    # ✅ gunakan RANGE supaya konsisten dengan laporan & (biasanya) sudah filter access
    def bal(code: str) -> float:
        try:
            return float(_account_balance_range(code, dfrom, dto))
        except Exception:
            # fallback: kalau fungsi ini belum ada/beda nama
            return float(_account_balance(code))

    def sum_by_type(t: str) -> float:
        accs = Account.query.filter(Account.type == t).all()
        total = 0.0
        for a in accs:
            b = bal(a.code)
            # Pendapatan biasanya kredit (balance negatif) -> dibalik jadi positif
            if t in ("Pendapatan", "Pendapatan Lain"):
                total += -b
            else:
                total += b
        return float(total)

    rev_main = sum_by_type("Pendapatan")
    hpp = sum_by_type("HPP")
    op_exp = sum_by_type("Beban")
    rev_other = sum_by_type("Pendapatan Lain")
    exp_other = sum_by_type("Beban Lain")

    gross_profit = rev_main - hpp
    operating_profit = gross_profit - op_exp
    net_profit = operating_profit + rev_other - exp_other

    # ===== Top Beban Operasional (ambil yg terbesar) =====
    exp_accounts = Account.query.filter(Account.type == "Beban").all()
    tmp = []
    for a in exp_accounts:
        amt = bal(a.code)
        if amt and amt > 0:
            tmp.append((a.name, float(amt)))
    tmp.sort(key=lambda x: x[1], reverse=True)
    tmp = tmp[:5]
    top_exp_labels = [x[0] for x in tmp]
    top_exp_values = [x[1] for x in tmp]

    # ===== Kas & Bank =====
    cash_accounts = Account.query.filter(Account.type == "Kas & Bank").order_by(Account.code.asc()).all()
    cash_labels = []
    cash_values = []
    cash_total = 0.0
    for a in cash_accounts:
        b = bal(a.code)
        cash_labels.append(f"{a.code} {a.name}")
        cash_values.append(float(b))
        cash_total += float(b)

    # ===== Pie Chart data (pakai ABS biar pie tidak error/aneh) =====
    chart_pl_labels = [
        "Pendapatan Usaha",
        "HPP",
        "Beban Operasional",
        "Pend. Luar Usaha",
        "Beban Luar Usaha",
        "Laba Bersih",
    ]
    chart_pl_values = [
        abs(float(rev_main)),
        abs(float(hpp)),
        abs(float(op_exp)),
        abs(float(rev_other)),
        abs(float(exp_other)),
        abs(float(net_profit)),
    ]

    return render_template(
        "dashboard.html",
        access=acc,
        remaining_hours=remaining_hours,
        cash_total=cash_total,
        rev_main=rev_main,
        hpp=hpp,
        op_exp=op_exp,
        net_profit=net_profit,
        chart_pl={"labels": chart_pl_labels, "values": chart_pl_values},
        chart_top_exp={"labels": top_exp_labels, "values": [abs(x) for x in top_exp_values]},
        chart_cash={"labels": cash_labels, "values": cash_values},  # biar label minus bisa diproses template
    )

@bp.get("/expired")
def expired():
    code = session.get(SESSION_KEY)
    acc = AccessCode.query.filter_by(code=code).first() if code else None
    return render_template("expired.html", access=acc)


@bp.post("/logout")
def logout():
    session.pop(SESSION_KEY, None)
    flash("Keluar.", "success")
    return redirect(url_for("main.enter_code"))


# ============================================================
# Master Data
# ============================================================
@bp.get("/master")
def master_home():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))
    return render_template("master_home.html")


@bp.route("/master/accounts", methods=["GET", "POST"])
def master_accounts():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    if request.method == "POST":
        code = (request.form.get("code") or "").strip()
        name = (request.form.get("name") or "").strip()
        atype = (request.form.get("type") or "").strip()

        if not code or not name or not atype:
            flash("Kode, Nama, dan Tipe akun wajib diisi.", "error")
            return redirect(url_for("main.master_accounts"))

        if Account.query.filter_by(code=code).first():
            flash("Kode akun sudah ada.", "error")
            return redirect(url_for("main.master_accounts"))

        db.session.add(Account(code=code, name=name, type=atype))
        db.session.commit()
        flash("Akun berhasil ditambahkan.", "success")
        return redirect(url_for("main.master_accounts"))

    accounts = Account.query.order_by(Account.code.asc()).all()
    return render_template("master_accounts.html", accounts=accounts)


@bp.post("/master/accounts/seed")
def seed_accounts():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    standard_accounts = [
        ("10011", "Kas", "Kas & Bank"),
        ("10021", "Bank", "Kas & Bank"),
        ("10031", "Piutang Usaha", "Akun Piutang"),
        ("10041", "Piutang Karyawan", "Aktiva Lancar Lain"),
        ("10042", "PPN Masukan", "Aktiva Lancar Lain"),
        ("10051", "Persediaan", "Persediaan"),
        ("10061", "Peralatan", "Aktiva Tetap"),
        ("10071", "Akum. Penyusutan Peralatan", "Akum. Peny."),
        ("20011", "Hutang Usaha", "Akun Hutang"),
        ("20021", "Hutang Lain", "Hutang Lancar Lain"),
        ("20022", "PPN Keluaran", "Hutang Lancar Lain"),
        ("20031", "Hutang Bank", "Hutang Jk. Panjang"),
        ("30011", "Modal", "Ekuitas"),
        ("30021", "Prive/Deviden", "Ekuitas"),
        ("30031", "Laba Ditahan", "Ekuitas"),
        ("40011", "Penjualan", "Pendapatan"),
        ("50011", "Beban Pokok Dapur", "HPP"),
        ("60011", "Biaya Gaji & Upah", "Beban"),
        ("60012", "Biaya Listrik", "Beban"),
        ("60013", "Biaya Promosi", "Beban"),
        ("60014", "Biaya Komisi", "Beban"),
        ("60015", "Biaya Perlengkapan Dapur", "Beban"),
        ("60016", "Biaya ATK", "Beban"),
        ("60017", "Biaya Pengiriman", "Beban"),
        ("60018", "Biaya Transportasi", "Beban"),
        ("60019", "Biaya Legalitas & Perizinan", "Beban"),
        ("60020", "Biaya PAM", "Beban"),
        ("60021", "Biaya Kebersihan Keamanan", "Beban"),
        ("60022", "Biaya Pajak", "Beban"),
        ("60099", "Biaya Lain-lain", "Beban"),
        ("70011", "Pendapatan Bunga Bank", "Pendapatan Lain"),
        ("80011", "Biaya Adm Bank", "Beban Lain"),
    ]

    inserted = 0
    skipped = 0

    for code, name, atype in standard_accounts:
        if Account.query.filter_by(code=code).first():
            skipped += 1
            continue
        db.session.add(Account(code=code, name=name, type=atype))
        inserted += 1

    db.session.commit()
    flash(f"Import akun standar selesai. Ditambah: {inserted}, dilewati: {skipped}.", "success")
    return redirect(url_for("main.master_accounts"))


@bp.route("/master/suppliers", methods=["GET", "POST"])
def master_suppliers():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        address = (request.form.get("address") or "").strip()

        if not name:
            flash("Nama supplier wajib diisi.", "error")
            return redirect(url_for("main.master_suppliers"))

        db.session.add(Supplier(name=name, phone=phone or None, address=address or None))
        db.session.commit()
        flash("Supplier berhasil ditambahkan.", "success")
        return redirect(url_for("main.master_suppliers"))

    suppliers = Supplier.query.order_by(Supplier.name.asc()).all()
    return render_template("master_suppliers.html", suppliers=suppliers)


@bp.route("/master/items", methods=["GET", "POST"])
def master_items():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        category = (request.form.get("category") or "").strip()
        unit = (request.form.get("unit") or "").strip()
        min_stock = (request.form.get("min_stock") or "0").strip()

        if not name or not unit:
            flash("Nama bahan dan satuan wajib diisi.", "error")
            return redirect(url_for("main.master_items"))

        try:
            min_stock_val = float(min_stock)
        except ValueError:
            flash("Minimal stok harus angka.", "error")
            return redirect(url_for("main.master_items"))

        db.session.add(Item(name=name, category=category or None, unit=unit, min_stock=min_stock_val))
        db.session.commit()
        flash("Bahan berhasil ditambahkan.", "success")
        return redirect(url_for("main.master_items"))

    items = Item.query.order_by(Item.name.asc()).all()
    return render_template("master_items.html", items=items)


# ============================================================
# Kas
# ============================================================
@bp.route("/cash", methods=["GET", "POST"])
def cash_home():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    accounts = Account.query.order_by(Account.code.asc()).all()

    if request.method == "POST":
        date_str = (request.form.get("date") or "").strip()
        direction = (request.form.get("direction") or "").strip()
        cash_code = (request.form.get("cash_account") or "").strip()
        counter_code = (request.form.get("counter_account") or "").strip()
        amount_str = (request.form.get("amount") or "").strip()
        memo = (request.form.get("memo") or "").strip()

        if not date_str or direction not in ("in", "out") or not cash_code or not counter_code or not amount_str:
            flash("Tanggal, tipe, akun kas/bank, akun lawan, dan nominal wajib diisi.", "error")
            return redirect(url_for("main.cash_home"))

        try:
            amount = float(amount_str)
            if amount <= 0:
                raise ValueError()
        except ValueError:
            flash("Nominal harus angka > 0.", "error")
            return redirect(url_for("main.cash_home"))

        cash_acc = Account.query.filter_by(code=cash_code).first()
        counter_acc = Account.query.filter_by(code=counter_code).first()
        if not cash_acc or not counter_acc:
            flash("Akun tidak valid. Pastikan sudah ada di COA.", "error")
            return redirect(url_for("main.cash_home"))

        tx = CashTransaction(
            date=_parse_date(date_str),
            direction=direction,
            cash_account_code=cash_acc.code,
            cash_account_name=cash_acc.name,
            counter_account_code=counter_acc.code,
            counter_account_name=counter_acc.name,
            amount=amount,
            memo=memo or None,
        )
        db.session.add(tx)
        db.session.flush()

        entry = _create_journal_for_cash(tx)
        tx.journal_entry_id = entry.id

        db.session.commit()
        flash("Transaksi kas tersimpan & jurnal otomatis dibuat.", "success")
        return redirect(url_for("main.cash_home"))

    txs = CashTransaction.query.order_by(CashTransaction.date.desc(), CashTransaction.id.desc()).limit(50).all()
    return render_template("cash_home.html", accounts=accounts, txs=txs)

@bp.route("/cash/<int:tx_id>/edit", methods=["GET", "POST"])
def cash_edit(tx_id: int):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    tx = CashTransaction.query.get_or_404(tx_id)
    accounts = Account.query.order_by(Account.code.asc()).all()

    if request.method == "POST":
        date_str = (request.form.get("date") or "").strip()
        direction = (request.form.get("direction") or "").strip()
        cash_code = (request.form.get("cash_account") or "").strip()
        counter_code = (request.form.get("counter_account") or "").strip()
        amount_str = (request.form.get("amount") or "").strip()
        memo = (request.form.get("memo") or "").strip()

        if not date_str or direction not in ("in", "out") or not cash_code or not counter_code or not amount_str:
            flash("Tanggal, tipe, akun kas/bank, akun lawan, dan nominal wajib diisi.", "error")
            return redirect(url_for("main.cash_edit", tx_id=tx_id))

        try:
            amount = float(amount_str)
            if amount <= 0:
                raise ValueError()
        except ValueError:
            flash("Nominal harus angka > 0.", "error")
            return redirect(url_for("main.cash_edit", tx_id=tx_id))

        cash_acc = Account.query.filter_by(code=cash_code).first()
        counter_acc = Account.query.filter_by(code=counter_code).first()
        if not cash_acc or not counter_acc:
            flash("Akun tidak valid.", "error")
            return redirect(url_for("main.cash_edit", tx_id=tx_id))

        # 1) hapus JournalEntry lama (kalau ada)
        if getattr(tx, "journal_entry_id", None):
            old_entry = JournalEntry.query.get(tx.journal_entry_id)
            if old_entry:
                db.session.delete(old_entry)
                db.session.flush()
            tx.journal_entry_id = None

        # 2) update transaksi
        tx.date = _parse_date(date_str)
        tx.direction = direction
        tx.cash_account_code = cash_acc.code
        tx.cash_account_name = cash_acc.name
        tx.counter_account_code = counter_acc.code
        tx.counter_account_name = counter_acc.name
        tx.amount = amount
        tx.memo = memo or None

        db.session.flush()

        # 3) buat ulang jurnal
        entry = _create_journal_for_cash(tx)
        tx.journal_entry_id = entry.id

        db.session.commit()
        flash("Transaksi kas berhasil diupdate.", "success")
        return redirect(url_for("main.cash_home"))

    return render_template("cash_edit.html", tx=tx, accounts=accounts)


@bp.post("/cash/<int:tx_id>/delete")
def cash_delete(tx_id: int):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    tx = CashTransaction.query.get_or_404(tx_id)

    # hapus journal entry terkait
    if getattr(tx, "journal_entry_id", None):
        entry = JournalEntry.query.get(tx.journal_entry_id)
        if entry:
            db.session.delete(entry)

    db.session.delete(tx)
    db.session.commit()
    flash("Transaksi kas berhasil dihapus.", "success")
    return redirect(url_for("main.cash_home"))


# ============================================================
# Jurnal (dengan filter tanggal)
# ============================================================
@bp.get("/journals")
def journals_list():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    dfrom, dto = _get_date_range_from_request()

    entries = (
        JournalEntry.query.filter(JournalEntry.date >= dfrom, JournalEntry.date <= dto)
        .order_by(JournalEntry.date.desc(), JournalEntry.id.desc())
        .limit(200)
        .all()
    )
    return render_template(
        "journals_list.html",
        entries=entries,
        dfrom=dfrom.strftime("%Y-%m-%d"),
        dto=dto.strftime("%Y-%m-%d"),
    )


@bp.get("/journals/<int:entry_id>")
def journals_detail(entry_id: int):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    entry = JournalEntry.query.get_or_404(entry_id)
    return render_template("journals_detail.html", entry=entry)


# ============================================================
# Purchase (hutang)
# ============================================================
@bp.route("/purchase", methods=["GET", "POST"])
def purchase_home():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    suppliers = Supplier.query.order_by(Supplier.name.asc()).all()
    items = Item.query.order_by(Item.name.asc()).all()

    if request.method == "POST":
        date_str = request.form.get("date")
        supplier_id = request.form.get("supplier_id")
        memo = request.form.get("memo")

        item_id = request.form.get("item_id")
        qty = request.form.get("qty")
        price = request.form.get("price")

        if not date_str or not item_id or not qty or not price:
            flash("Tanggal, bahan, qty, dan harga wajib diisi.", "error")
            return redirect(url_for("main.purchase_home"))

        try:
            qty = float(qty)
            price = float(price)
            if qty <= 0 or price <= 0:
                raise ValueError()
        except ValueError:
            flash("Qty dan harga harus angka > 0.", "error")
            return redirect(url_for("main.purchase_home"))

        item = Item.query.get(int(item_id))
        if not item:
            flash("Bahan tidak valid.", "error")
            return redirect(url_for("main.purchase_home"))

        subtotal = qty * price

        purchase = Purchase(date=_parse_date(date_str), total_amount=subtotal, memo=memo or None)

        if supplier_id:
            supplier = Supplier.query.get(int(supplier_id))
            if supplier:
                purchase.supplier_id = supplier.id
                purchase.supplier_name = supplier.name

        db.session.add(purchase)
        db.session.flush()

        pitem = PurchaseItem(
            purchase_id=purchase.id,
            item_id=item.id,
            item_name=item.name,
            qty=qty,
            price=price,
            subtotal=subtotal,
        )
        db.session.add(pitem)

        # update stok & avg cost
        total_cost_existing = float(item.stock_qty or 0) * float(item.avg_cost or 0)
        total_cost_new = qty * price
        new_qty = float(item.stock_qty or 0) + qty

        item.avg_cost = (total_cost_existing + total_cost_new) / new_qty
        item.stock_qty = new_qty

        entry = _create_journal_for_purchase(purchase)
        purchase.journal_entry_id = entry.id

        db.session.commit()
        flash("Pembelian tersimpan, stok bertambah, hutang tercatat.", "success")
        return redirect(url_for("main.purchase_home"))

    purchases = Purchase.query.order_by(Purchase.date.desc()).limit(20).all()
    return render_template("purchase_home.html", suppliers=suppliers, items=items, purchases=purchases)

# ============================================================
# PURCHASE: Helpers untuk reverse stok + rebuild jurnal
# ============================================================

def _recalc_item_avg_cost(item: Item):
    """
    Optional: kalau kamu mau punya util khusus.
    Di sini kita biarkan avg_cost sesuai proses reverse+apply.
    """
    return


def _reverse_purchase_stock(pitem: PurchaseItem):
    """
    Mengembalikan dampak pembelian lama:
    - Kurangi stok item sebesar qty lama
    - Recompute avg_cost dengan pendekatan biaya total (weighted)
    Catatan: ini pendekatan yang konsisten dengan cara kamu menambah avg_cost.
    """
    item = Item.query.get(pitem.item_id)
    if not item:
        return

    old_qty = float(pitem.qty or 0)
    old_price = float(pitem.price or 0)

    if old_qty <= 0:
        return

    cur_qty = float(item.stock_qty or 0)
    cur_avg = float(item.avg_cost or 0)

    # total cost saat ini
    total_cost_cur = cur_qty * cur_avg

    # total cost yang dulu ditambahkan oleh pembelian ini
    total_cost_old = old_qty * old_price

    # reverse
    new_qty = cur_qty - old_qty
    if new_qty <= 0:
        # kalau stok jadi 0 atau negatif, set ke 0 dan avg_cost reset
        item.stock_qty = 0.0
        item.avg_cost = 0.0
        return

    # total cost setelah reverse
    new_total_cost = total_cost_cur - total_cost_old
    if new_total_cost < 0:
        # guard jika data lama tidak konsisten
        new_total_cost = 0.0

    item.stock_qty = new_qty
    item.avg_cost = new_total_cost / new_qty if new_qty else 0.0


def _apply_purchase_stock(item: Item, qty: float, price: float):
    """
    Terapkan pembelian baru:
    - Tambah stok
    - Update avg_cost weighted
    """
    qty = float(qty or 0)
    price = float(price or 0)
    if qty <= 0:
        return

    cur_qty = float(item.stock_qty or 0)
    cur_avg = float(item.avg_cost or 0)

    total_cost_existing = cur_qty * cur_avg
    total_cost_new = qty * price
    new_qty = cur_qty + qty

    item.stock_qty = new_qty
    item.avg_cost = (total_cost_existing + total_cost_new) / new_qty if new_qty else 0.0


def _rebuild_journal_for_purchase(purchase: Purchase):
    _delete_journal_entry(getattr(purchase, "journal_entry_id", None))
    db.session.flush()
    entry = _create_journal_for_purchase(purchase)
    purchase.journal_entry_id = entry.id


# ============================================================
# PURCHASE: Edit / Delete
# ============================================================

@bp.route("/purchase/<int:purchase_id>/edit", methods=["GET", "POST"])
def purchase_edit(purchase_id: int):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    purchase = Purchase.query.get_or_404(purchase_id)

    # Asumsi: satu purchase punya satu PurchaseItem (sesuai kode kamu)
    pitem = PurchaseItem.query.filter_by(purchase_id=purchase.id).first()
    if not pitem:
        flash("Item pembelian tidak ditemukan.", "error")
        return redirect(url_for("main.purchase_home"))

    suppliers = Supplier.query.order_by(Supplier.name.asc()).all()
    items = Item.query.order_by(Item.name.asc()).all()

    if request.method == "POST":
        date_str = (request.form.get("date") or "").strip()
        supplier_id = (request.form.get("supplier_id") or "").strip()
        memo = (request.form.get("memo") or "").strip()

        item_id = (request.form.get("item_id") or "").strip()
        qty_str = (request.form.get("qty") or "").strip()
        price_str = (request.form.get("price") or "").strip()

        if not date_str or not item_id or not qty_str or not price_str:
            flash("Tanggal, bahan, qty, dan harga wajib diisi.", "error")
            return redirect(url_for("main.purchase_edit", purchase_id=purchase.id))

        try:
            qty = float(qty_str)
            price = float(price_str)
            if qty <= 0 or price <= 0:
                raise ValueError()
        except ValueError:
            flash("Qty dan harga harus angka > 0.", "error")
            return redirect(url_for("main.purchase_edit", purchase_id=purchase.id))

        new_item = Item.query.get(int(item_id))
        if not new_item:
            flash("Bahan tidak valid.", "error")
            return redirect(url_for("main.purchase_edit", purchase_id=purchase.id))

        # --- STEP 1: reverse stok dari pembelian lama
        _reverse_purchase_stock(pitem)

        # --- STEP 2: update purchase + pitem
        purchase.date = _parse_date(date_str)
        purchase.memo = memo or None

        if supplier_id:
            sup = Supplier.query.get(int(supplier_id))
            if sup:
                purchase.supplier_id = sup.id
                purchase.supplier_name = sup.name
            else:
                purchase.supplier_id = None
                purchase.supplier_name = None
        else:
            purchase.supplier_id = None
            purchase.supplier_name = None

        # update pitem (bisa ganti item)
        pitem.item_id = new_item.id
        pitem.item_name = new_item.name
        pitem.qty = qty
        pitem.price = price
        pitem.subtotal = qty * price

        # purchase total
        purchase.total_amount = pitem.subtotal

        # --- STEP 3: apply stok baru ke item baru
        _apply_purchase_stock(new_item, qty, price)

        # --- STEP 4: rebuild jurnal pembelian
        _rebuild_journal_for_purchase(purchase)

        db.session.commit()
        flash("Pembelian berhasil diupdate. Stok & jurnal sudah disesuaikan.", "success")
        return redirect(url_for("main.purchase_home"))

    return render_template(
        "purchase_edit.html",
        purchase=purchase,
        pitem=pitem,
        suppliers=suppliers,
        items=items,
    )


@bp.post("/purchase/<int:purchase_id>/delete")
def purchase_delete(purchase_id: int):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    purchase = Purchase.query.get_or_404(purchase_id)
    pitem = PurchaseItem.query.filter_by(purchase_id=purchase.id).first()

    # reverse stok dulu
    if pitem:
        _reverse_purchase_stock(pitem)

    # hapus jurnal
    _delete_journal_entry(getattr(purchase, "journal_entry_id", None))

    # hapus item pembelian
    if pitem:
        db.session.delete(pitem)

    # hapus purchase
    db.session.delete(purchase)
    db.session.commit()

    flash("Pembelian dihapus. Stok & jurnal sudah dikembalikan.", "success")
    return redirect(url_for("main.purchase_home"))


# ============================================================
# AP Payment
# ============================================================
@bp.route("/ap-payment", methods=["GET", "POST"])
def ap_payment_home():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    purchases = Purchase.query.order_by(Purchase.date.desc()).all()
    cash_accounts = Account.query.filter(Account.type == "Kas & Bank").order_by(Account.code.asc()).all()

    if request.method == "POST":
        date_str = request.form.get("date")
        purchase_id = request.form.get("purchase_id")
        cash_code = request.form.get("cash_account")
        amount_str = request.form.get("amount")
        memo = request.form.get("memo")

        if not date_str or not cash_code or not amount_str:
            flash("Tanggal, akun kas, dan nominal wajib diisi.", "error")
            return redirect(url_for("main.ap_payment_home"))

        try:
            amount = float(amount_str)
            if amount <= 0:
                raise ValueError()
        except ValueError:
            flash("Nominal harus angka > 0.", "error")
            return redirect(url_for("main.ap_payment_home"))

        cash_acc = Account.query.filter_by(code=cash_code).first()
        if not cash_acc:
            flash("Akun kas/bank tidak valid.", "error")
            return redirect(url_for("main.ap_payment_home"))

        payment = APayment(
            date=_parse_date(date_str),
            amount=amount,
            cash_account_code=cash_acc.code,
            cash_account_name=cash_acc.name,
            memo=memo or None,
        )

        if purchase_id:
            purchase = Purchase.query.get(int(purchase_id))
            if purchase:
                payment.purchase_id = purchase.id
                payment.supplier_name = purchase.supplier_name
                if amount >= float(purchase.total_amount or 0):
                    purchase.is_paid = True

        db.session.add(payment)
        db.session.flush()

        entry = _create_journal_for_ap_payment(payment)
        payment.journal_entry_id = entry.id

        db.session.commit()
        flash("Pembayaran hutang berhasil dicatat.", "success")
        return redirect(url_for("main.ap_payment_home"))

    payments = APayment.query.order_by(APayment.date.desc()).limit(20).all()
    return render_template(
        "ap_payment_home.html",
        purchases=purchases,
        cash_accounts=cash_accounts,
        payments=payments,
    )

# ============================================================
# AP PAYMENT: Edit / Delete
# ============================================================

@bp.route("/ap-payment/<int:payment_id>/edit", methods=["GET", "POST"])
def ap_payment_edit(payment_id: int):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    payment = APayment.query.get_or_404(payment_id)

    purchases = Purchase.query.order_by(Purchase.date.desc()).all()
    cash_accounts = Account.query.filter(
        Account.type == "Kas & Bank"
    ).order_by(Account.code.asc()).all()

    if request.method == "POST":
        date_str = (request.form.get("date") or "").strip()
        purchase_id = (request.form.get("purchase_id") or "").strip()
        cash_code = (request.form.get("cash_account") or "").strip()
        amount_str = (request.form.get("amount") or "").strip()
        memo = (request.form.get("memo") or "").strip()

        if not date_str or not cash_code or not amount_str:
            flash("Tanggal, akun kas, dan nominal wajib diisi.", "error")
            return redirect(url_for("main.ap_payment_edit", payment_id=payment.id))

        try:
            amount = float(amount_str)
            if amount <= 0:
                raise ValueError()
        except ValueError:
            flash("Nominal harus angka > 0.", "error")
            return redirect(url_for("main.ap_payment_edit", payment_id=payment.id))

        # === STEP 1: rollback status pembelian lama
        if payment.purchase_id:
            old_purchase = Purchase.query.get(payment.purchase_id)
            if old_purchase:
                old_purchase.is_paid = False

        # === STEP 2: hapus jurnal lama
        _delete_journal_entry(payment.journal_entry_id)
        db.session.flush()

        # === STEP 3: update payment
        payment.date = _parse_date(date_str)
        payment.amount = amount
        payment.memo = memo or None

        cash_acc = Account.query.filter_by(code=cash_code).first()
        payment.cash_account_code = cash_acc.code
        payment.cash_account_name = cash_acc.name

        if purchase_id:
            purchase = Purchase.query.get(int(purchase_id))
            if purchase:
                payment.purchase_id = purchase.id
                payment.supplier_name = purchase.supplier_name
                if amount >= float(purchase.total_amount or 0):
                    purchase.is_paid = True
        else:
            payment.purchase_id = None
            payment.supplier_name = None

        # === STEP 4: buat jurnal baru
        entry = _create_journal_for_ap_payment(payment)
        payment.journal_entry_id = entry.id

        db.session.commit()
        flash("Pembayaran hutang berhasil diupdate.", "success")
        return redirect(url_for("main.ap_payment_home"))

    return render_template(
        "ap_payment_edit.html",
        payment=payment,
        purchases=purchases,
        cash_accounts=cash_accounts,
    )


@bp.post("/ap-payment/<int:payment_id>/delete")
def ap_payment_delete(payment_id: int):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    payment = APayment.query.get_or_404(payment_id)

    # rollback status hutang
    if payment.purchase_id:
        purchase = Purchase.query.get(payment.purchase_id)
        if purchase:
            purchase.is_paid = False

    # hapus jurnal
    _delete_journal_entry(payment.journal_entry_id)

    db.session.delete(payment)
    db.session.commit()

    flash("Pembayaran hutang dihapus. Jurnal & status hutang dikembalikan.", "success")
    return redirect(url_for("main.ap_payment_home"))

# ============================================================
# PENJUALAN (SIMPLE) - pakai CashTransaction dengan tag memo [SALE]
# Auto Jurnal: Debit (Kas/Bank/Piutang) vs Kredit (Pendapatan)
# ============================================================

def _sale_memo(customer: str | None, note: str | None) -> str:
    customer = (customer or "").strip()
    note = (note or "").strip()
    parts = []
    if customer:
        parts.append(customer)
    if note:
        parts.append(note)
    suffix = " - ".join(parts) if parts else ""
    return "[SALE]" + (f" {suffix}" if suffix else "")


@bp.route("/sales", methods=["GET", "POST"])
def sales_home():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    # pilihan akun debit: Kas/Bank + Piutang
    debit_accounts = (
        Account.query.filter(Account.type.in_(["Kas & Bank", "Akun Piutang"]))
        .order_by(Account.code.asc())
        .all()
    )
    # pilihan akun kredit: Pendapatan + Pendapatan Lain (kalau mau)
    revenue_accounts = (
        Account.query.filter(Account.type.in_(["Pendapatan", "Pendapatan Lain"]))
        .order_by(Account.code.asc())
        .all()
    )

    if request.method == "POST":
        date_str = (request.form.get("date") or "").strip()
        customer = (request.form.get("customer_name") or "").strip()
        debit_code = (request.form.get("debit_account") or "").strip()
        credit_code = (request.form.get("revenue_account") or "").strip()
        amount_str = (request.form.get("amount") or "").strip()
        note = (request.form.get("memo") or "").strip()

        if not date_str or not debit_code or not credit_code or not amount_str:
            flash("Tanggal, akun debit, akun pendapatan, dan nominal wajib diisi.", "error")
            return redirect(url_for("main.sales_home"))

        try:
            amount = float(amount_str)
            if amount <= 0:
                raise ValueError()
        except ValueError:
            flash("Nominal harus angka > 0.", "error")
            return redirect(url_for("main.sales_home"))

        debit_acc = Account.query.filter_by(code=debit_code).first()
        credit_acc = Account.query.filter_by(code=credit_code).first()
        if not debit_acc or not credit_acc:
            flash("Akun tidak valid.", "error")
            return redirect(url_for("main.sales_home"))

        # simpan sebagai CashTransaction (masuk)
        tx = CashTransaction(
            date=_parse_date(date_str),
            direction="in",
            cash_account_code=debit_acc.code,
            cash_account_name=debit_acc.name,
            counter_account_code=credit_acc.code,
            counter_account_name=credit_acc.name,
            amount=amount,
            memo=_sale_memo(customer, note),
        )
        db.session.add(tx)
        db.session.flush()

        # jurnal otomatis
        entry = _create_journal_for_cash(tx)
        tx.journal_entry_id = entry.id

        db.session.commit()
        flash("Penjualan tersimpan & jurnal otomatis dibuat.", "success")
        return redirect(url_for("main.sales_home"))

    # List hanya transaksi yang bertag [SALE]
    sales = (
        CashTransaction.query.filter(
            CashTransaction.direction == "in",
            CashTransaction.memo.like("[SALE]%"),
        )
        .order_by(CashTransaction.date.desc(), CashTransaction.id.desc())
        .limit(100)
        .all()
    )

    return render_template(
        "sales_home.html",
        debit_accounts=debit_accounts,
        revenue_accounts=revenue_accounts,
        sales=sales,
        today=datetime.utcnow().strftime("%Y-%m-%d"),
    )


@bp.route("/sales/<int:tx_id>/edit", methods=["GET", "POST"])
def sales_edit(tx_id: int):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    tx = CashTransaction.query.get_or_404(tx_id)

    # validasi: cuma boleh edit transaksi penjualan
    if not (tx.direction == "in" and (tx.memo or "").startswith("[SALE]")):
        flash("Transaksi ini bukan penjualan.", "error")
        return redirect(url_for("main.sales_home"))

    debit_accounts = (
        Account.query.filter(Account.type.in_(["Kas & Bank", "Akun Piutang"]))
        .order_by(Account.code.asc())
        .all()
    )
    revenue_accounts = (
        Account.query.filter(Account.type.in_(["Pendapatan", "Pendapatan Lain"]))
        .order_by(Account.code.asc())
        .all()
    )

    if request.method == "POST":
        date_str = (request.form.get("date") or "").strip()
        customer = (request.form.get("customer_name") or "").strip()
        debit_code = (request.form.get("debit_account") or "").strip()
        credit_code = (request.form.get("revenue_account") or "").strip()
        amount_str = (request.form.get("amount") or "").strip()
        note = (request.form.get("memo") or "").strip()

        if not date_str or not debit_code or not credit_code or not amount_str:
            flash("Tanggal, akun debit, akun pendapatan, dan nominal wajib diisi.", "error")
            return redirect(url_for("main.sales_edit", tx_id=tx.id))

        try:
            amount = float(amount_str)
            if amount <= 0:
                raise ValueError()
        except ValueError:
            flash("Nominal harus angka > 0.", "error")
            return redirect(url_for("main.sales_edit", tx_id=tx.id))

        debit_acc = Account.query.filter_by(code=debit_code).first()
        credit_acc = Account.query.filter_by(code=credit_code).first()
        if not debit_acc or not credit_acc:
            flash("Akun tidak valid.", "error")
            return redirect(url_for("main.sales_edit", tx_id=tx.id))

        # Hapus jurnal lama lalu buat baru (paling aman biar balance)
        old_entry_id = tx.journal_entry_id
        if old_entry_id:
            old_entry = JournalEntry.query.get(old_entry_id)
            if old_entry:
                db.session.delete(old_entry)
                db.session.flush()

        # Update tx
        tx.date = _parse_date(date_str)
        tx.cash_account_code = debit_acc.code
        tx.cash_account_name = debit_acc.name
        tx.counter_account_code = credit_acc.code
        tx.counter_account_name = credit_acc.name
        tx.amount = amount
        tx.memo = _sale_memo(customer, note)

        db.session.flush()

        # Buat jurnal baru
        new_entry = _create_journal_for_cash(tx)
        tx.journal_entry_id = new_entry.id

        db.session.commit()
        flash("Penjualan berhasil diupdate.", "success")
        return redirect(url_for("main.sales_home"))

    # split memo biar form enak (ambil setelah [SALE])
    raw = (tx.memo or "").replace("[SALE]", "").strip()
    # kita gak bisa perfect parsing, jadi taruh raw ke memo saja
    return render_template(
        "sales_edit.html",
        tx=tx,
        debit_accounts=debit_accounts,
        revenue_accounts=revenue_accounts,
        raw_memo=raw,
    )


@bp.post("/sales/<int:tx_id>/delete")
def sales_delete(tx_id: int):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    tx = CashTransaction.query.get_or_404(tx_id)

    if not (tx.direction == "in" and (tx.memo or "").startswith("[SALE]")):
        flash("Transaksi ini bukan penjualan.", "error")
        return redirect(url_for("main.sales_home"))

    # hapus jurnal dulu
    if tx.journal_entry_id:
        entry = JournalEntry.query.get(tx.journal_entry_id)
        if entry:
            db.session.delete(entry)

    db.session.delete(tx)
    db.session.commit()

    flash("Penjualan dihapus.", "success")
    return redirect(url_for("main.sales_home"))

# ============================================================
# Sales Invoice
# ============================================================

# ============================================================
# AR Payments
# ============================================================
@bp.route("/ar/payments", methods=["GET", "POST"])
def ar_payment_home():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    cash_accounts = Account.query.filter(Account.type == "Kas & Bank").order_by(Account.code.asc()).all()
    open_invoices = SalesInvoice.query.filter(SalesInvoice.status != "paid").order_by(SalesInvoice.date.desc()).all()

    if request.method == "POST":
        date_str = (request.form.get("date") or "").strip()
        invoice_id = (request.form.get("invoice_id") or "").strip()
        cash_code = (request.form.get("cash_account") or "").strip()
        amount_str = (request.form.get("amount") or "").strip()
        memo = (request.form.get("memo") or "").strip()

        if not date_str or not invoice_id or not cash_code or not amount_str:
            flash("Tanggal, invoice, akun kas/bank, dan nominal wajib diisi.", "error")
            return redirect(url_for("main.ar_payment_home"))

        inv = SalesInvoice.query.get(int(invoice_id))
        if not inv:
            flash("Invoice tidak ditemukan.", "error")
            return redirect(url_for("main.ar_payment_home"))

        cash_acc = Account.query.filter_by(code=cash_code).first()
        if not cash_acc:
            flash("Akun kas/bank tidak valid.", "error")
            return redirect(url_for("main.ar_payment_home"))

        try:
            amt = float(amount_str)
            if amt <= 0:
                raise ValueError()
        except ValueError:
            flash("Nominal harus angka > 0.", "error")
            return redirect(url_for("main.ar_payment_home"))

        remaining = float(inv.total_amount or 0) - float(inv.paid_amount or 0)
        if amt > remaining:
            flash(f"Nominal melebihi sisa piutang (sisa: Rp {remaining:,.0f}).", "error")
            return redirect(url_for("main.ar_payment_home"))

        pay = ARPayment(
            date=_parse_date(date_str),
            invoice_id=inv.id,
            invoice_no=inv.invoice_no,
            cash_account_code=cash_acc.code,
            cash_account_name=cash_acc.name,
            amount=amt,
            memo=memo or None,
        )
        db.session.add(pay)
        db.session.flush()

        entry = _create_journal_for_ar_payment(pay, inv)
        pay.journal_entry_id = entry.id

        inv.paid_amount = float(inv.paid_amount or 0) + amt
        if inv.paid_amount >= float(inv.total_amount or 0):
            inv.status = "paid"
        else:
            inv.status = "partial"

        db.session.commit()
        flash("Pembayaran piutang tersimpan & jurnal otomatis dibuat.", "success")
        return redirect(url_for("main.ar_payment_home"))

    payments = ARPayment.query.order_by(ARPayment.date.desc(), ARPayment.id.desc()).limit(50).all()
    return render_template(
        "ar_payment_home.html",
        payments=payments,
        cash_accounts=cash_accounts,
        open_invoices=open_invoices,
    )

@bp.route("/ar-settlement", methods=["GET", "POST"])
def ar_settlement_home():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    cash_accounts = (
        Account.query.filter(Account.type == "Kas & Bank")
        .order_by(Account.code.asc())
        .all()
    )
    ar_accounts = (
        Account.query.filter(Account.type == "Akun Piutang")
        .order_by(Account.code.asc())
        .all()
    )

    if request.method == "POST":
        date_str = (request.form.get("date") or "").strip()
        customer = (request.form.get("customer_name") or "").strip()
        cash_code = (request.form.get("cash_account") or "").strip()
        ar_code = (request.form.get("ar_account") or "").strip()
        amount_str = (request.form.get("amount") or "").strip()
        note = (request.form.get("memo") or "").strip()

        if not date_str or not cash_code or not ar_code or not amount_str:
            flash("Tanggal, akun kas/bank, akun piutang, dan nominal wajib diisi.", "error")
            return redirect(url_for("main.ar_payment_home"))

        try:
            amount = float(amount_str)
            if amount <= 0:
                raise ValueError()
        except ValueError:
            flash("Nominal harus angka > 0.", "error")
            return redirect(url_for("main.ar_payment_home"))

        cash_acc = Account.query.filter_by(code=cash_code).first()
        ar_acc = Account.query.filter_by(code=ar_code).first()
        if not cash_acc or not ar_acc:
            flash("Akun tidak valid.", "error")
            return redirect(url_for("main.ar_payment_home"))

        # Simpan sebagai transaksi kas MASUK:
        # Debit: Kas/Bank
        # Kredit: Piutang
        tx = CashTransaction(
            date=_parse_date(date_str),
            direction="in",
            cash_account_code=cash_acc.code,
            cash_account_name=cash_acc.name,
            counter_account_code=ar_acc.code,
            counter_account_name=ar_acc.name,
            amount=amount,
            memo=_arpay_memo(customer, note),
        )
        db.session.add(tx)
        db.session.flush()

        entry = _create_journal_for_cash(tx)
        tx.journal_entry_id = entry.id

        db.session.commit()
        flash("Pelunasan piutang tersimpan & jurnal otomatis dibuat.", "success")
        return redirect(url_for("main.ar_payment_home"))

    payments = (
        CashTransaction.query.filter(
            CashTransaction.direction == "in",
            CashTransaction.memo.like("[AR]%"),
        )
        .order_by(CashTransaction.date.desc(), CashTransaction.id.desc())
        .limit(100)
        .all()
    )

    # (opsional) tampilkan saldo piutang per akun (biar user bisa cek outstanding)
    ar_balances = []
    for a in ar_accounts:
        bal = _account_balance(a.code)  # saldo all-time
        # untuk piutang: saldo normalnya debit (+)
        if bal != 0:
            ar_balances.append((a, float(bal)))

    return render_template(
        "ar_settlement_home.html",
        cash_accounts=cash_accounts,
        ar_accounts=ar_accounts,
        payments=payments,
        ar_balances=ar_balances,
        today=datetime.utcnow().strftime("%Y-%m-%d"),
    )

# =========================
# Pelunasan Piutang: Edit
# =========================
@bp.route("/ar/settlement/<int:payment_id>/edit", methods=["GET", "POST"])
def ar_settlement_edit(payment_id: int):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    pay = ARPayment.query.get_or_404(payment_id)

    # pilihan akun kas/bank
    cash_accounts = (
        Account.query.filter(Account.type == "Kas & Bank")
        .order_by(Account.code.asc())
        .all()
    )

    if request.method == "POST":
        date_str = (request.form.get("date") or "").strip()
        cash_code = (request.form.get("cash_account") or "").strip()
        amount_str = (request.form.get("amount") or "").strip()
        memo = (request.form.get("memo") or "").strip()

        if not date_str or not cash_code or not amount_str:
            flash("Tanggal, akun kas/bank, dan nominal wajib diisi.", "error")
            return redirect(url_for("main.ar_settlement_edit", payment_id=payment_id))

        try:
            amount = float(amount_str)
            if amount <= 0:
                raise ValueError()
        except ValueError:
            flash("Nominal harus angka > 0.", "error")
            return redirect(url_for("main.ar_settlement_edit", payment_id=payment_id))

        cash_acc = Account.query.filter_by(code=cash_code).first()
        if not cash_acc:
            flash("Akun kas/bank tidak valid.", "error")
            return redirect(url_for("main.ar_settlement_edit", payment_id=payment_id))

        # ---- Update payment
        pay.date = _parse_date(date_str)
        pay.cash_account_code = cash_acc.code
        pay.cash_account_name = cash_acc.name
        pay.amount = amount
        pay.memo = memo or None

        # ---- Update jurnal jika ada
        if pay.journal_entry_id:
            entry = JournalEntry.query.get(pay.journal_entry_id)
            if entry:
                entry.date = pay.date
                entry.memo = pay.memo or entry.memo

                # update line: Debit Kas/Bank, Kredit Piutang
                # Asumsi: line[0]=Debit kas/bank, line[1]=Kredit piutang
                # Kalau urutan beda di datamu, kita bisa cari berdasarkan debit/credit > 0
                lines = entry.lines or []
                if len(lines) >= 2:
                    # debit kas/bank
                    lines[0].account_code = cash_acc.code
                    lines[0].account_name = cash_acc.name
                    lines[0].debit = amount
                    lines[0].credit = 0

                    # kredit piutang (pakai data existing pay.invoice/ar_account kalau kamu punya)
                    lines[1].debit = 0
                    lines[1].credit = amount

        db.session.commit()
        flash("Pembayaran piutang berhasil diupdate.", "success")
        return redirect(url_for("main.ar_payment_home"))

    return render_template(
        "ar_settlement_edit.html",
        pay=pay,
        cash_accounts=cash_accounts,
        date_value=pay.date.strftime("%Y-%m-%d") if pay.date else datetime.utcnow().strftime("%Y-%m-%d"),
    )


# =========================
# Pelunasan Piutang: Delete
# =========================
@bp.post("/ar/settlement/<int:payment_id>/delete")
def ar_settlement_delete(payment_id: int):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    pay = ARPayment.query.get_or_404(payment_id)

    # hapus jurnal kalau ada
    if pay.journal_entry_id:
        entry = JournalEntry.query.get(pay.journal_entry_id)
        if entry:
            # hapus lines dulu (kalau relationship tidak cascade)
            for ln in list(entry.lines):
                db.session.delete(ln)
            db.session.delete(entry)

    db.session.delete(pay)
    db.session.commit()

    flash("Pembayaran piutang berhasil dihapus.", "success")
    return redirect(url_for("main.ar_payment_home"))

# ============================================================
# Expenses (kas keluar ke akun beban)
# ============================================================
@bp.route("/expenses", methods=["GET", "POST"])
def expenses_home():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    cash_accounts = Account.query.filter(Account.type == "Kas & Bank").order_by(Account.code.asc()).all()
    expense_accounts = Account.query.filter(Account.type.in_(["Beban", "Beban Lain"])).order_by(Account.code.asc()).all()

    if request.method == "POST":
        date_str = (request.form.get("date") or "").strip()
        cash_code = (request.form.get("cash_account") or "").strip()
        exp_code = (request.form.get("expense_account") or "").strip()
        amount_str = (request.form.get("amount") or "").strip()
        memo = (request.form.get("memo") or "").strip()

        if not date_str or not cash_code or not exp_code or not amount_str:
            flash("Tanggal, akun kas, akun beban, dan nominal wajib diisi.", "error")
            return redirect(url_for("main.expenses_home"))

        try:
            amount = float(amount_str)
            if amount <= 0:
                raise ValueError()
        except ValueError:
            flash("Nominal harus angka > 0.", "error")
            return redirect(url_for("main.expenses_home"))

        cash_acc = Account.query.filter_by(code=cash_code).first()
        exp_acc = Account.query.filter_by(code=exp_code).first()
        if not cash_acc or not exp_acc:
            flash("Akun tidak valid.", "error")
            return redirect(url_for("main.expenses_home"))

        tx = CashTransaction(
            date=_parse_date(date_str),
            direction="out",
            cash_account_code=cash_acc.code,
            cash_account_name=cash_acc.name,
            counter_account_code=exp_acc.code,
            counter_account_name=exp_acc.name,
            amount=amount,
            memo=memo or None,
        )
        db.session.add(tx)
        db.session.flush()

        entry = _create_journal_for_cash(tx)
        tx.journal_entry_id = entry.id

        db.session.commit()
        flash("Biaya operasional tersimpan & jurnal dibuat.", "success")
        return redirect(url_for("main.expenses_home"))

    txs = (
        CashTransaction.query.filter_by(direction="out")
        .order_by(CashTransaction.date.desc(), CashTransaction.id.desc())
        .limit(50)
        .all()
    )
    return render_template("expenses_home.html", cash_accounts=cash_accounts, expense_accounts=expense_accounts, txs=txs)

# ============================================================
# EDIT/HAPUS: Expenses (Biaya) - CashTransaction direction="out"
# ============================================================

@bp.route("/expenses/<int:tx_id>/edit", methods=["GET", "POST"])
def expense_edit(tx_id: int):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    tx = CashTransaction.query.get_or_404(tx_id)
    if tx.direction != "out":
        flash("Transaksi ini bukan transaksi biaya.", "error")
        return redirect(url_for("main.expenses_home"))

    cash_accounts = Account.query.filter(Account.type == "Kas & Bank").order_by(Account.code.asc()).all()
    expense_accounts = Account.query.filter(Account.type.in_(["Beban", "Beban Lain"])).order_by(Account.code.asc()).all()

    if request.method == "POST":
        date_str = (request.form.get("date") or "").strip()
        cash_code = (request.form.get("cash_account") or "").strip()
        exp_code = (request.form.get("expense_account") or "").strip()
        amount_str = (request.form.get("amount") or "").strip()
        memo = (request.form.get("memo") or "").strip()

        if not date_str or not cash_code or not exp_code or not amount_str:
            flash("Tanggal, akun kas, akun beban, dan nominal wajib diisi.", "error")
            return redirect(url_for("main.expense_edit", tx_id=tx.id))

        try:
            amount = float(amount_str)
            if amount <= 0:
                raise ValueError()
        except ValueError:
            flash("Nominal harus angka > 0.", "error")
            return redirect(url_for("main.expense_edit", tx_id=tx.id))

        cash_acc = Account.query.filter_by(code=cash_code).first()
        exp_acc = Account.query.filter_by(code=exp_code).first()
        if not cash_acc or not exp_acc:
            flash("Akun tidak valid.", "error")
            return redirect(url_for("main.expense_edit", tx_id=tx.id))

        # update transaksi
        tx.date = _parse_date(date_str)
        tx.cash_account_code = cash_acc.code
        tx.cash_account_name = cash_acc.name
        tx.counter_account_code = exp_acc.code
        tx.counter_account_name = exp_acc.name
        tx.amount = amount
        tx.memo = memo or None

        # rebuild jurnal: hapus journal lama, buat baru
        if getattr(tx, "journal_entry_id", None):
            old = JournalEntry.query.get(tx.journal_entry_id)
            if old:
                db.session.delete(old)
                db.session.flush()

        entry = _create_journal_for_cash(tx)
        tx.journal_entry_id = entry.id

        db.session.commit()
        flash("Transaksi biaya berhasil diupdate.", "success")
        return redirect(url_for("main.expenses_home"))

    return render_template(
        "expense_edit.html",
        tx=tx,
        cash_accounts=cash_accounts,
        expense_accounts=expense_accounts,
    )


@bp.post("/expenses/<int:tx_id>/delete")
def expense_delete(tx_id: int):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    tx = CashTransaction.query.get_or_404(tx_id)
    if tx.direction != "out":
        flash("Transaksi ini bukan transaksi biaya.", "error")
        return redirect(url_for("main.expenses_home"))

    # hapus jurnal terkait
    if getattr(tx, "journal_entry_id", None):
        old = JournalEntry.query.get(tx.journal_entry_id)
        if old:
            db.session.delete(old)

    db.session.delete(tx)
    db.session.commit()
    flash("Transaksi biaya berhasil dihapus.", "success")
    return redirect(url_for("main.expenses_home"))


# ============================================================
# Stock Usage
# ============================================================
@bp.route("/stock-usage", methods=["GET", "POST"])
def stock_usage_home():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    items = Item.query.order_by(Item.name.asc()).all()
    hpp_accounts = Account.query.filter(Account.type.in_(["HPP", "Beban"])).order_by(Account.code.asc()).all()

    if request.method == "POST":
        date_str = (request.form.get("date") or "").strip()
        item_id = (request.form.get("item_id") or "").strip()
        qty_str = (request.form.get("qty") or "").strip()
        hpp_code = (request.form.get("hpp_account") or "").strip()
        memo = (request.form.get("memo") or "").strip()

        if not date_str or not item_id or not qty_str or not hpp_code:
            flash("Tanggal, bahan, qty, dan akun HPP wajib diisi.", "error")
            return redirect(url_for("main.stock_usage_home"))

        try:
            qty = float(qty_str)
            if qty <= 0:
                raise ValueError()
        except ValueError:
            flash("Qty harus angka > 0.", "error")
            return redirect(url_for("main.stock_usage_home"))

        item = Item.query.get(int(item_id))
        if not item:
            flash("Bahan tidak valid.", "error")
            return redirect(url_for("main.stock_usage_home"))

        if float(item.stock_qty or 0) < qty:
            flash(f"Stok tidak cukup. Stok saat ini: {item.stock_qty:g} {item.unit}.", "error")
            return redirect(url_for("main.stock_usage_home"))

        hpp_acc = Account.query.filter_by(code=hpp_code).first()
        if not hpp_acc:
            flash("Akun HPP tidak valid.", "error")
            return redirect(url_for("main.stock_usage_home"))

        unit_cost = float(item.avg_cost or 0)
        total_cost = qty * unit_cost

        u = StockUsage(
            date=_parse_date(date_str),
            item_id=item.id,
            item_name=item.name,
            qty=qty,
            unit_cost=unit_cost,
            total_cost=total_cost,
            hpp_account_code=hpp_acc.code,
            hpp_account_name=hpp_acc.name,
            memo=memo or None,
        )
        db.session.add(u)
        db.session.flush()

        item.stock_qty = float(item.stock_qty or 0) - qty

        entry = _create_journal_for_stock_usage(u)
        u.journal_entry_id = entry.id

        db.session.commit()
        flash("Pemakaian stok tersimpan, persediaan berkurang, jurnal dibuat.", "success")
        return redirect(url_for("main.stock_usage_home"))

    usages = StockUsage.query.order_by(StockUsage.date.desc(), StockUsage.id.desc()).limit(50).all()
    return render_template("stock_usage_home.html", items=items, hpp_accounts=hpp_accounts, usages=usages)

# ============================================================
# EDIT/HAPUS: Stock Usage
# ============================================================

@bp.route("/stock-usage/<int:usage_id>/edit", methods=["GET", "POST"])
def stock_usage_edit(usage_id: int):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    usage = StockUsage.query.get_or_404(usage_id)

    items = Item.query.order_by(Item.name.asc()).all()
    hpp_accounts = Account.query.filter(Account.type.in_(["HPP", "Beban"])).order_by(Account.code.asc()).all()

    if request.method == "POST":
        date_str = (request.form.get("date") or "").strip()
        item_id_str = (request.form.get("item_id") or "").strip()
        qty_str = (request.form.get("qty") or "").strip()
        hpp_code = (request.form.get("hpp_account") or "").strip()
        memo = (request.form.get("memo") or "").strip()

        if not date_str or not item_id_str or not qty_str or not hpp_code:
            flash("Tanggal, bahan, qty, dan akun HPP wajib diisi.", "error")
            return redirect(url_for("main.stock_usage_edit", usage_id=usage.id))

        try:
            new_qty = float(qty_str)
            if new_qty <= 0:
                raise ValueError()
        except ValueError:
            flash("Qty harus angka > 0.", "error")
            return redirect(url_for("main.stock_usage_edit", usage_id=usage.id))

        new_item = Item.query.get(int(item_id_str))
        if not new_item:
            flash("Bahan tidak valid.", "error")
            return redirect(url_for("main.stock_usage_edit", usage_id=usage.id))

        hpp_acc = Account.query.filter_by(code=hpp_code).first()
        if not hpp_acc:
            flash("Akun HPP tidak valid.", "error")
            return redirect(url_for("main.stock_usage_edit", usage_id=usage.id))

        # ===== 1) Kembalikan stok dari pemakaian lama
        old_item = Item.query.get(usage.item_id)
        old_qty = float(usage.qty or 0)
        if old_item:
            old_item.stock_qty = float(old_item.stock_qty or 0) + old_qty

        # ===== 2) Validasi stok cukup untuk pemakaian baru
        # Karena stok sudah dibalikin, sekarang cek new_item cukup
        if float(new_item.stock_qty or 0) < new_qty:
            flash(
                f"Stok tidak cukup setelah penyesuaian. Stok tersedia: {float(new_item.stock_qty or 0):g} {new_item.unit}.",
                "error",
            )
            db.session.rollback()
            return redirect(url_for("main.stock_usage_edit", usage_id=usage.id))

        # ===== 3) Apply pemakaian baru (kurangi stok)
        unit_cost = float(new_item.avg_cost or 0)
        total_cost = new_qty * unit_cost

        new_item.stock_qty = float(new_item.stock_qty or 0) - new_qty

        usage.date = _parse_date(date_str)
        usage.item_id = new_item.id
        usage.item_name = new_item.name
        usage.qty = new_qty
        usage.unit_cost = unit_cost
        usage.total_cost = total_cost
        usage.hpp_account_code = hpp_acc.code
        usage.hpp_account_name = hpp_acc.name
        usage.memo = memo or None

        # ===== 4) Rebuild jurnal: hapus yang lama, buat baru
        if getattr(usage, "journal_entry_id", None):
            old_entry = JournalEntry.query.get(usage.journal_entry_id)
            if old_entry:
                db.session.delete(old_entry)
                db.session.flush()

        entry = _create_journal_for_stock_usage(usage)
        usage.journal_entry_id = entry.id

        db.session.commit()
        flash("Pemakaian stok berhasil diupdate.", "success")
        return redirect(url_for("main.stock_usage_home"))

    return render_template(
        "stock_usage_edit.html",
        usage=usage,
        items=items,
        hpp_accounts=hpp_accounts,
    )


@bp.post("/stock-usage/<int:usage_id>/delete")
def stock_usage_delete(usage_id: int):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    usage = StockUsage.query.get_or_404(usage_id)

    # ===== balikin stok
    item = Item.query.get(usage.item_id)
    if item:
        item.stock_qty = float(item.stock_qty or 0) + float(usage.qty or 0)

    # ===== hapus jurnal terkait
    if getattr(usage, "journal_entry_id", None):
        old_entry = JournalEntry.query.get(usage.journal_entry_id)
        if old_entry:
            db.session.delete(old_entry)

    db.session.delete(usage)
    db.session.commit()
    flash("Pemakaian stok berhasil dihapus (stok & jurnal dikembalikan).", "success")
    return redirect(url_for("main.stock_usage_home"))


# ============================================================
# REPORT: Buku Besar (filter tanggal)
# ============================================================
@bp.get("/reports/ledger")
def report_ledger():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    accounts = Account.query.order_by(Account.code.asc()).all()
    selected_code = (request.args.get("account") or "").strip()

    from_str = (request.args.get("from") or "").strip()
    to_str = (request.args.get("to") or "").strip()

    from_date = _parse_date(from_str) if from_str else None
    to_date = _parse_date(to_str) if to_str else None

    lines = []
    balance = 0.0

    if selected_code:
        fk = _jl_entry_fk()
        q = (
            JournalLine.query
            .join(JournalEntry, fk == JournalEntry.id)
            .filter(JournalLine.account_code == selected_code)
        )

        if from_date:
            q = q.filter(JournalEntry.date >= from_date)
        if to_date:
            q = q.filter(JournalEntry.date <= to_date)

        lines = q.order_by(JournalEntry.date.asc(), JournalLine.id.asc()).all()
        balance = sum((ln.debit or 0) - (ln.credit or 0) for ln in lines)

    return render_template(
        "report_ledger.html",
        accounts=accounts,
        selected_code=selected_code or None,
        lines=lines,
        balance=balance,
        from_date=from_str,
        to_date=to_str,
    )


# =========================
# Excel Helpers
# =========================
_THIN = Side(style="thin", color="999999")
_BORDER = Border(left=_THIN, right=_THIN, top=_THIN, bottom=_THIN)

def _autosize_columns(ws, max_width=60):
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            try:
                val = "" if cell.value is None else str(cell.value)
                max_len = max(max_len, len(val))
            except Exception:
                pass
        ws.column_dimensions[col_letter].width = min(max_width, max(10, max_len + 2))

def _style_header_row(ws, row_idx=1):
    fill = PatternFill("solid", fgColor="1F4E79")
    font = Font(color="FFFFFF", bold=True)
    align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    for cell in ws[row_idx]:
        cell.fill = fill
        cell.font = font
        cell.alignment = align
        cell.border = _BORDER
    ws.row_dimensions[row_idx].height = 20

def _style_table_cells(ws, start_row, end_row, start_col, end_col):
    align_left = Alignment(horizontal="left", vertical="top", wrap_text=True)
    align_right = Alignment(horizontal="right", vertical="top")
    for r in range(start_row, end_row + 1):
        for c in range(start_col, end_col + 1):
            cell = ws.cell(row=r, column=c)
            cell.border = _BORDER
            if c in (3, 4, 5):
                cell.alignment = align_right
            else:
                cell.alignment = align_left

def _fmt_idr_excel(cell):
    cell.number_format = "#,##0"

def _get_entry_date_and_memo(line: JournalLine):
    """
    Aman untuk model yang kadang pakai relationship ln.entry, kadang tidak.
    """
    je = None
    if hasattr(line, "entry") and line.entry is not None:
        je = line.entry
    else:
        if hasattr(line, "entry_id"):
            je = JournalEntry.query.get(line.entry_id)
        elif hasattr(line, "journal_entry_id"):
            je = JournalEntry.query.get(line.journal_entry_id)
    if not je:
        return None, "-"
    return je.date, (je.memo or "-")


# =========================
# EXPORT: Buku Besar ke Excel (Per Akun)
# URL: /export/ledger.xlsx?account=10011&from=2026-02-01&to=2026-02-02
# =========================
@bp.get("/export/ledger.xlsx")
def export_ledger_xlsx():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    code = (request.args.get("account") or "").strip()
    if not code:
        flash("Pilih akun dulu untuk export Buku Besar.", "error")
        return redirect(url_for("main.report_ledger"))

    from_dt, to_dt_excl, from_str, to_str = _get_date_range_args()

    account = Account.query.filter_by(code=code).first()
    acc_name = account.name if account else ""

    q = _jl_base_query(from_dt, to_dt_excl).filter(JournalLine.account_code == code)
    q = q.order_by(JournalEntry.date.asc(), JournalLine.id.asc())
    lines = q.all()

    wb = Workbook()
    ws = wb.active
    ws.title = "Buku Besar"

    ws["A1"] = "Buku Besar"
    ws["A1"].font = Font(bold=True, size=14)
    ws["A2"] = f"Akun: {code} - {acc_name}".strip(" -")
    ws["A3"] = f"Dapur: {acc.dapur_name or 'Dapur MBG'}"

    periode = "Periode: "
    if from_str and to_str:
        periode += f"{from_str} s/d {to_str}"
    elif from_str and not to_str:
        periode += f"mulai {from_str}"
    elif (not from_str) and to_str:
        periode += f"sampai {to_str}"
    else:
        periode += "Seluruh Periode"
    ws["A4"] = periode

    start_row = 6
    ws.append([""] * 5)  # row 5
    ws.append(["Tanggal", "Keterangan", "Debit", "Kredit", "Saldo Berjalan"])  # row 6
    _style_header_row(ws, start_row)

    saldo = 0.0
    r = start_row + 1
    for ln in lines:
        dt, memo = _get_entry_date_and_memo(ln)
        debit = float(ln.debit or 0)
        credit = float(ln.credit or 0)
        saldo += (debit - credit)

        ws.cell(row=r, column=1, value=dt.date().isoformat() if dt else "-")
        ws.cell(row=r, column=2, value=memo)
        ws.cell(row=r, column=3, value=debit)
        ws.cell(row=r, column=4, value=credit)
        ws.cell(row=r, column=5, value=saldo)

        _fmt_idr_excel(ws.cell(row=r, column=3))
        _fmt_idr_excel(ws.cell(row=r, column=4))
        _fmt_idr_excel(ws.cell(row=r, column=5))
        r += 1

    if r > start_row + 1:
        _style_table_cells(ws, start_row + 1, r - 1, 1, 5)

    total_row = r + 1
    ws.cell(row=total_row, column=2, value="SALDO AKHIR").font = Font(bold=True)
    ws.cell(row=total_row, column=5, value=saldo).font = Font(bold=True)
    _fmt_idr_excel(ws.cell(row=total_row, column=5))

    _autosize_columns(ws)

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)

    filename = f"buku_besar_{code}.xlsx"
    return send_file(
        bio,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# =========================
# EXPORT: Buku Besar ke Excel (Semua Akun, per sheet)
# URL: /export/ledger-all.xlsx?from=2026-02-01&to=2026-02-02
# =========================
@bp.get("/export/ledger-all.xlsx")
def export_ledger_all_xlsx():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    from_dt, to_dt_excl, from_str, to_str = _get_date_range_args()

    periode = "Periode: "
    if from_str and to_str:
        periode += f"{from_str} s/d {to_str}"
    elif from_str and not to_str:
        periode += f"mulai {from_str}"
    elif (not from_str) and to_str:
        periode += f"sampai {to_str}"
    else:
        periode += "Seluruh Periode"

    accounts = Account.query.order_by(Account.code.asc()).all()

    wb = Workbook()
    ws_sum = wb.active
    ws_sum.title = "Ringkasan"

    ws_sum["A1"] = "Buku Besar - Semua Akun"
    ws_sum["A1"].font = Font(bold=True, size=14)
    ws_sum["A2"] = f"Dapur: {acc.dapur_name or 'Dapur MBG'}"
    ws_sum["A3"] = periode

    ws_sum.append([])
    ws_sum.append(["Kode", "Nama Akun", "Saldo (Debit - Kredit)"])
    header_row = ws_sum.max_row
    _style_header_row(ws_sum, header_row)

    ringkasan_start = ws_sum.max_row + 1
    ringkasan_row = ringkasan_start

    for a in accounts:
        q = _jl_base_query(from_dt, to_dt_excl).filter(JournalLine.account_code == a.code)
        q = q.order_by(JournalEntry.date.asc(), JournalLine.id.asc())
        lines = q.all()
        if not lines:
            continue

        sheet_name = f"{a.code}"
        if sheet_name in wb.sheetnames:
            sheet_name = f"{a.code}_{(a.name or '')[:10]}"
        ws = wb.create_sheet(title=sheet_name[:31])

        ws["A1"] = "Buku Besar"
        ws["A1"].font = Font(bold=True, size=14)
        ws["A2"] = f"Akun: {a.code} - {a.name}"
        ws["A3"] = f"Dapur: {acc.dapur_name or 'Dapur MBG'}"
        ws["A4"] = periode

        start_row = 6
        ws.append([""] * 5)
        ws.append(["Tanggal", "Keterangan", "Debit", "Kredit", "Saldo Berjalan"])
        _style_header_row(ws, start_row)

        saldo = 0.0
        r = start_row + 1
        for ln in lines:
            dt, memo = _get_entry_date_and_memo(ln)
            debit = float(ln.debit or 0)
            credit = float(ln.credit or 0)
            saldo += (debit - credit)

            ws.cell(row=r, column=1, value=dt.date().isoformat() if dt else "-")
            ws.cell(row=r, column=2, value=memo)
            ws.cell(row=r, column=3, value=debit)
            ws.cell(row=r, column=4, value=credit)
            ws.cell(row=r, column=5, value=saldo)

            _fmt_idr_excel(ws.cell(row=r, column=3))
            _fmt_idr_excel(ws.cell(row=r, column=4))
            _fmt_idr_excel(ws.cell(row=r, column=5))
            r += 1

        if r > start_row + 1:
            _style_table_cells(ws, start_row + 1, r - 1, 1, 5)

        ws.cell(row=r + 1, column=2, value="SALDO AKHIR").font = Font(bold=True)
        ws.cell(row=r + 1, column=5, value=saldo).font = Font(bold=True)
        _fmt_idr_excel(ws.cell(row=r + 1, column=5))

        _autosize_columns(ws)

        # ringkasan
        ws_sum.cell(row=ringkasan_row, column=1, value=a.code)
        ws_sum.cell(row=ringkasan_row, column=2, value=a.name)
        ws_sum.cell(row=ringkasan_row, column=3, value=saldo)
        _fmt_idr_excel(ws_sum.cell(row=ringkasan_row, column=3))
        ringkasan_row += 1

    if ringkasan_row > ringkasan_start:
        _style_table_cells(ws_sum, ringkasan_start, ringkasan_row - 1, 1, 3)

    _autosize_columns(ws_sum)

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)

    return send_file(
        bio,
        as_attachment=True,
        download_name="buku_besar_semua_akun.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# ============================================================
# REPORT: Laba Rugi (filter tanggal, struktur standar)
# ============================================================
@bp.get("/reports/profit-loss")
def report_profit_loss():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    dfrom, dto = _get_date_range_from_request()

    rev_main = Account.query.filter(Account.type == "Pendapatan").order_by(Account.code.asc()).all()
    hpp_accounts = Account.query.filter(Account.type == "HPP").order_by(Account.code.asc()).all()
    op_exp = Account.query.filter(Account.type == "Beban").order_by(Account.code.asc()).all()
    rev_other = Account.query.filter(Account.type == "Pendapatan Lain").order_by(Account.code.asc()).all()
    exp_other = Account.query.filter(Account.type == "Beban Lain").order_by(Account.code.asc()).all()

    def amt_revenue(a):
        return -_account_balance_range(a.code, dfrom, dto)

    def amt_expense(a):
        return _account_balance_range(a.code, dfrom, dto)

    rev_main_data, total_rev_main = [], 0.0
    for a in rev_main:
        amt = float(amt_revenue(a))
        if amt != 0:
            rev_main_data.append((a, amt))
            total_rev_main += amt

    hpp_data, total_hpp = [], 0.0
    for a in hpp_accounts:
        amt = float(amt_expense(a))
        if amt != 0:
            hpp_data.append((a, amt))
            total_hpp += amt

    gross_profit = total_rev_main - total_hpp

    op_exp_data, total_op_exp = [], 0.0
    for a in op_exp:
        amt = float(amt_expense(a))
        if amt != 0:
            op_exp_data.append((a, amt))
            total_op_exp += amt

    operating_profit = gross_profit - total_op_exp

    rev_other_data, total_rev_other = [], 0.0
    for a in rev_other:
        amt = float(amt_revenue(a))
        if amt != 0:
            rev_other_data.append((a, amt))
            total_rev_other += amt

    exp_other_data, total_exp_other = [], 0.0
    for a in exp_other:
        amt = float(amt_expense(a))
        if amt != 0:
            exp_other_data.append((a, amt))
            total_exp_other += amt

    net_profit = operating_profit + total_rev_other - total_exp_other

    return render_template(
        "report_profit_loss.html",
        rev_main_data=rev_main_data,
        hpp_data=hpp_data,
        op_exp_data=op_exp_data,
        rev_other_data=rev_other_data,
        exp_other_data=exp_other_data,
        total_rev_main=total_rev_main,
        total_hpp=total_hpp,
        gross_profit=gross_profit,
        total_op_exp=total_op_exp,
        operating_profit=operating_profit,
        total_rev_other=total_rev_other,
        total_exp_other=total_exp_other,
        net_profit=net_profit,
        dfrom=dfrom.strftime("%Y-%m-%d"),
        dto=dto.strftime("%Y-%m-%d"),
    )


# ============================================================
# REPORT: Neraca (filter tanggal + laba tahun berjalan)
# ============================================================
@bp.get("/reports/balance-sheet")
def report_balance_sheet():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    # as_of=YYYY-MM-DD (default hari ini)
    as_of_str = (request.args.get("as_of") or "").strip()
    as_of_date = _parse_ymd(as_of_str) or datetime.utcnow().date()
    # jadikan "to exclusive" untuk query < (as_of + 1 hari)
    to_dt_excl = datetime.combine(as_of_date + timedelta(days=1), datetime.min.time())

    def bal_upto(code: str) -> float:
        # dari awal sampai as_of (inclusive)
        q = JournalLine.query.join(JournalEntry, JournalLine.entry_id == JournalEntry.id).filter(
            JournalLine.account_code == code,
            JournalEntry.date < to_dt_excl,
        )
        debit = q.with_entities(db.func.sum(JournalLine.debit)).scalar() or 0
        credit = q.with_entities(db.func.sum(JournalLine.credit)).scalar() or 0
        return float(debit) - float(credit)

    # kelompok akun neraca
    assets = Account.query.filter(
        Account.type.in_([
            "Kas & Bank",
            "Akun Piutang",
            "Aktiva Lancar Lain",
            "Persediaan",
            "Aktiva Tetap",
            "Akum. Peny.",
        ])
    ).order_by(Account.code.asc()).all()

    liabilities = Account.query.filter(
        Account.type.in_(["Akun Hutang", "Hutang Lancar Lain", "Hutang Jk. Panjang"])
    ).order_by(Account.code.asc()).all()

    equities = Account.query.filter(Account.type == "Ekuitas").order_by(Account.code.asc()).all()

    asset_data, liab_data, eq_data = [], [], []
    total_assets = total_liab = total_eq = 0.0

    # ASET: normal debit (bal_upto apa adanya)
    for a in assets:
        amt = float(bal_upto(a.code))
        if amt != 0:
            asset_data.append((a, amt))
            total_assets += amt

    # LIABILITAS: normal kredit => tampilkan positif => -(debit-credit)
    for a in liabilities:
        amt = -float(bal_upto(a.code))
        if amt != 0:
            liab_data.append((a, amt))
            total_liab += amt

    # EKUITAS: normal kredit => tampilkan positif
    for a in equities:
        amt = -float(bal_upto(a.code))
        if amt != 0:
            eq_data.append((a, amt))
            total_eq += amt

    # ===== NET PROFIT (dari awal sampai as_of) -> masuk ke ekuitas
    rev_accounts = Account.query.filter(Account.type == "Pendapatan").all()
    rev_other_accounts = Account.query.filter(Account.type == "Pendapatan Lain").all()
    hpp_accounts = Account.query.filter(Account.type == "HPP").all()
    exp_accounts = Account.query.filter(Account.type == "Beban").all()
    exp_other_accounts = Account.query.filter(Account.type == "Beban Lain").all()

    sum_rev = sum(float(bal_upto(a.code)) for a in rev_accounts)            # biasanya kredit => negatif
    sum_rev_other = sum(float(bal_upto(a.code)) for a in rev_other_accounts)
    sum_hpp = sum(float(bal_upto(a.code)) for a in hpp_accounts)            # biasanya debit => positif
    sum_exp = sum(float(bal_upto(a.code)) for a in exp_accounts)
    sum_exp_other = sum(float(bal_upto(a.code)) for a in exp_other_accounts)

    net_profit = (-sum_rev) + (-sum_rev_other) - (sum_hpp + sum_exp + sum_exp_other)

    if net_profit != 0:
        dummy = type("Tmp", (), {})()
        dummy.code = "99999"
        dummy.name = "Laba (Rugi) Sampai Tanggal Ini"
        dummy.type = "Ekuitas"
        eq_data.append((dummy, float(net_profit)))
        total_eq += float(net_profit)

    diff = float(total_assets) - float(total_liab + total_eq)

    return render_template(
        "report_balance_sheet.html",
        asset_data=asset_data,
        liab_data=liab_data,
        eq_data=eq_data,
        total_assets=total_assets,
        total_liab=total_liab,
        total_eq=total_eq,
        as_of=as_of_date.strftime("%Y-%m-%d"),
        as_of_display=as_of_date.strftime("%d %b %Y"),
        diff=diff,
    )


# ============================================================
# EXPORT PDF: Profit Loss (ikut filter tanggal)
# ============================================================
@bp.get("/export/profit-loss")
def export_profit_loss_pdf():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    dfrom, dto = _get_date_range_from_request()

    rev_main_accounts = Account.query.filter(Account.type == "Pendapatan").order_by(Account.code.asc()).all()
    hpp_accounts = Account.query.filter(Account.type == "HPP").order_by(Account.code.asc()).all()
    op_exp_accounts = Account.query.filter(Account.type == "Beban").order_by(Account.code.asc()).all()
    rev_other_accounts = Account.query.filter(Account.type == "Pendapatan Lain").order_by(Account.code.asc()).all()
    exp_other_accounts = Account.query.filter(Account.type == "Beban Lain").order_by(Account.code.asc()).all()

    def amt_revenue(a):
        return -_account_balance_range(a.code, dfrom, dto)

    def amt_expense(a):
        return _account_balance_range(a.code, dfrom, dto)

    rev_main_rows, total_rev_main = [], 0.0
    for a in rev_main_accounts:
        amt = float(amt_revenue(a))
        if amt != 0:
            rev_main_rows.append([a.name, fmt_idr(amt)])
            total_rev_main += amt

    hpp_rows, total_hpp = [], 0.0
    for a in hpp_accounts:
        amt = float(amt_expense(a))
        if amt != 0:
            hpp_rows.append([a.name, fmt_idr(amt)])
            total_hpp += amt

    gross_profit = total_rev_main - total_hpp

    op_exp_rows, total_op_exp = [], 0.0
    for a in op_exp_accounts:
        amt = float(amt_expense(a))
        if amt != 0:
            op_exp_rows.append([a.name, fmt_idr(amt)])
            total_op_exp += amt

    operating_profit = gross_profit - total_op_exp

    rev_other_rows, total_rev_other = [], 0.0
    for a in rev_other_accounts:
        amt = float(amt_revenue(a))
        if amt != 0:
            rev_other_rows.append([a.name, fmt_idr(amt)])
            total_rev_other += amt

    exp_other_rows, total_exp_other = [], 0.0
    for a in exp_other_accounts:
        amt = float(amt_expense(a))
        if amt != 0:
            exp_other_rows.append([a.name, fmt_idr(amt)])
            total_exp_other += amt

    net_profit = operating_profit + total_rev_other - total_exp_other

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        doc = pdf_doc(tmp.name)
        story = []

        header_block(
            story,
            "Laba/Rugi (Standar)",
            f"Periode: {dfrom.strftime('%d %b %Y')} s.d {dto.strftime('%d %b %Y')}",
            "Mata Uang: Indonesian Rupiah",
        )

        section_title(story, "PENDAPATAN USAHA")
        story.append(table_2col([["Deskripsi", "Nilai (IDR)"]] + rev_main_rows, header=True))
        story.append(table_2col([["Total Pendapatan Usaha", fmt_idr(total_rev_main)]]))

        section_title(story, "HARGA POKOK PENJUALAN")
        story.append(table_2col([["Deskripsi", "Nilai (IDR)"]] + hpp_rows, header=True))
        story.append(table_2col([["Total Harga Pokok Penjualan", fmt_idr(total_hpp)]]))

        story.append(table_2col([["LABA (RUGI) KOTOR", fmt_idr(gross_profit)]]))

        section_title(story, "BEBAN OPERASIONAL")
        story.append(table_2col([["Deskripsi", "Nilai (IDR)"]] + op_exp_rows, header=True))
        story.append(table_2col([["Total Beban Operasional", fmt_idr(total_op_exp)]]))

        story.append(table_2col([["LABA (RUGI) OPERASIONAL", fmt_idr(operating_profit)]]))

        section_title(story, "PENDAPATAN DI LUAR USAHA")
        story.append(table_2col([["Deskripsi", "Nilai (IDR)"]] + rev_other_rows, header=True))
        story.append(table_2col([["Total Pendapatan di Luar Usaha", fmt_idr(total_rev_other)]]))

        section_title(story, "BEBAN DI LUAR USAHA")
        story.append(table_2col([["Deskripsi", "Nilai (IDR)"]] + exp_other_rows, header=True))
        story.append(table_2col([["Total Beban di Luar Usaha", fmt_idr(total_exp_other)]]))

        story.append(table_2col([["LABA (RUGI) BERSIH", fmt_idr(net_profit)]]))

        doc.build(story, onFirstPage=footer_canvas(), onLaterPages=footer_canvas())
        return send_file(tmp.name, as_attachment=True, download_name="laba_rugi_standar.pdf")


# ============================================================
# EXPORT PDF: Balance Sheet (ikut filter tanggal + laba berjalan)
# ============================================================
@bp.get("/export/balance-sheet")
def export_balance_sheet_pdf():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    as_of_str = (request.args.get("as_of") or "").strip()
    as_of_date = _parse_ymd(as_of_str) or datetime.utcnow().date()
    to_dt_excl = datetime.combine(as_of_date + timedelta(days=1), datetime.min.time())

    def bal_upto(code: str) -> float:
        q = JournalLine.query.join(JournalEntry, JournalLine.entry_id == JournalEntry.id).filter(
            JournalLine.account_code == code,
            JournalEntry.date < to_dt_excl,
        )
        debit = q.with_entities(db.func.sum(JournalLine.debit)).scalar() or 0
        credit = q.with_entities(db.func.sum(JournalLine.credit)).scalar() or 0
        return float(debit) - float(credit)

    def build_rows(accounts, reverse=False):
        rows = [["Kode Akun", "Deskripsi", "Nilai (IDR)"]]
        total = 0.0
        for a in accounts:
            amt = float(bal_upto(a.code))
            if reverse:
                amt = -amt
            if amt != 0:
                rows.append([a.code, a.name, fmt_idr(amt)])
                total += amt
        return rows, total

    kas_bank = Account.query.filter(Account.type == "Kas & Bank").order_by(Account.code.asc()).all()
    piutang = Account.query.filter(Account.type == "Akun Piutang").order_by(Account.code.asc()).all()
    aset_lancar_lain = Account.query.filter(Account.type == "Aktiva Lancar Lain").order_by(Account.code.asc()).all()
    persediaan = Account.query.filter(Account.type == "Persediaan").order_by(Account.code.asc()).all()
    aset_tetap = Account.query.filter(Account.type == "Aktiva Tetap").order_by(Account.code.asc()).all()
    akum = Account.query.filter(Account.type == "Akum. Peny.").order_by(Account.code.asc()).all()

    hutang = Account.query.filter(
        Account.type.in_(["Akun Hutang", "Hutang Lancar Lain", "Hutang Jk. Panjang"])
    ).order_by(Account.code.asc()).all()
    ekuitas = Account.query.filter(Account.type == "Ekuitas").order_by(Account.code.asc()).all()

    # net profit sampai as_of
    rev_accounts = Account.query.filter(Account.type == "Pendapatan").all()
    rev_other_accounts = Account.query.filter(Account.type == "Pendapatan Lain").all()
    hpp_accounts = Account.query.filter(Account.type == "HPP").all()
    exp_accounts = Account.query.filter(Account.type == "Beban").all()
    exp_other_accounts = Account.query.filter(Account.type == "Beban Lain").all()

    sum_rev = sum(float(bal_upto(a.code)) for a in rev_accounts)
    sum_rev_other = sum(float(bal_upto(a.code)) for a in rev_other_accounts)
    sum_hpp = sum(float(bal_upto(a.code)) for a in hpp_accounts)
    sum_exp = sum(float(bal_upto(a.code)) for a in exp_accounts)
    sum_exp_other = sum(float(bal_upto(a.code)) for a in exp_other_accounts)
    net_profit = (-sum_rev) + (-sum_rev_other) - (sum_hpp + sum_exp + sum_exp_other)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        doc = pdf_doc(tmp.name)
        story = []

        header_block(
            story,
            "Neraca (Standar)",
            f"Per Tanggal: {as_of_date.strftime('%d %b %Y')}",
            "Mata Uang : Indonesian Rupiah",
        )

        section_title(story, "ASET")
        subsection_title(story, "ASET LANCAR")

        subsection_title(story, "Kas dan Setara Kas")
        rows, total_kas = build_rows(kas_bank, reverse=False)
        story.append(table_3col(rows))
        story.append(table_3col([["", "Jumlah Kas dan Setara Kas", fmt_idr(total_kas)]], header=False))

        subsection_title(story, "Piutang")
        rows, total_piutang = build_rows(piutang, reverse=False)
        story.append(table_3col(rows))
        story.append(table_3col([["", "Jumlah Piutang", fmt_idr(total_piutang)]], header=False))

        subsection_title(story, "Persediaan")
        rows, total_pers = build_rows(persediaan, reverse=False)
        story.append(table_3col(rows))
        story.append(table_3col([["", "Jumlah Persediaan", fmt_idr(total_pers)]], header=False))

        subsection_title(story, "Aset Lancar Lainnya")
        rows, total_lain = build_rows(aset_lancar_lain, reverse=False)
        story.append(table_3col(rows))
        story.append(table_3col([["", "Jumlah Aset Lancar Lainnya", fmt_idr(total_lain)]], header=False))

        total_aset_lancar = total_kas + total_piutang + total_pers + total_lain
        story.append(table_3col([["", "Jumlah Aset Lancar", fmt_idr(total_aset_lancar)]], header=False))

        subsection_title(story, "ASET TIDAK LANCAR")

        subsection_title(story, "Nilai Histori")
        rows, total_histori = build_rows(aset_tetap, reverse=False)
        story.append(table_3col(rows))
        story.append(table_3col([["", "Jumlah Nilai Histori", fmt_idr(total_histori)]], header=False))

        subsection_title(story, "Akumulasi Penyusutan")
        rows, total_akum = build_rows(akum, reverse=False)
        story.append(table_3col(rows))
        story.append(table_3col([["", "Jumlah Akumulasi Penyusutan", fmt_idr(total_akum)]], header=False))

        total_aset_tidak_lancar = total_histori + total_akum
        story.append(table_3col([["", "Jumlah Aset Tidak Lancar", fmt_idr(total_aset_tidak_lancar)]], header=False))

        total_aset = total_aset_lancar + total_aset_tidak_lancar
        story.append(table_3col([["", "JUMLAH ASET", fmt_idr(total_aset)]], header=False))

        section_title(story, "LIABILITAS DAN EKUITAS")
        subsection_title(story, "LIABILITAS")
        rows, total_liab = build_rows(hutang, reverse=True)
        story.append(table_3col(rows))
        story.append(table_3col([["", "Jumlah Kewajiban", fmt_idr(total_liab)]], header=False))

        subsection_title(story, "EKUITAS")
        rows, total_eq = build_rows(ekuitas, reverse=True)

        if net_profit != 0:
            rows.append(["99999", "Laba (Rugi) Sampai Tanggal Ini", fmt_idr(net_profit)])
            total_eq += float(net_profit)

        story.append(table_3col(rows))
        story.append(table_3col([["", "Jumlah Ekuitas", fmt_idr(total_eq)]], header=False))
        story.append(table_3col([["", "JUMLAH LIABILITAS DAN EKUITAS", fmt_idr(total_liab + total_eq)]], header=False))

        doc.build(story, onFirstPage=footer_canvas(), onLaterPages=footer_canvas())
        return send_file(tmp.name, as_attachment=True, download_name="neraca_standar.pdf")

# ============================================================
# EXPORT PDF: Invoice (tetap)
# ============================================================
@bp.get("/export/sales-invoice/<int:invoice_id>")
def export_sales_invoice_pdf(invoice_id):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    inv = SalesInvoice.query.get_or_404(invoice_id)

    rows = [["No", "Deskripsi", "Qty", "Satuan", "Harga", "Jumlah"]]
    for i, ln in enumerate(inv.lines, start=1):
        rows.append(
            [
                str(i),
                ln.description,
                f"{float(ln.qty or 0):,.2f}",
                ln.unit or "-",
                fmt_idr(float(ln.price or 0)),
                fmt_idr(float(ln.amount or 0)),
            ]
        )

    total = float(inv.total_amount or 0)
    paid = float(inv.paid_amount or 0)
    remaining = total - paid

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        doc = pdf_doc(tmp.name)
        story = []

        header_block(
            story,
            "INVOICE PENJUALAN",
            f"No: {inv.invoice_no}  |  Tgl: {inv.date.strftime('%d %b %Y')}",
            f"Dapur: {acc.dapur_name or '-'}",
        )

        section_title(story, "INFO CUSTOMER")
        story.append(
            table_2col(
                [
                    ["Customer", inv.customer_name],
                    ["No HP", inv.customer_phone or "-"],
                    ["Status", (inv.status or "").upper()],
                    ["Catatan", inv.notes or "-"],
                ]
            )
        )

        section_title(story, "AKUN")
        story.append(
            table_2col(
                [
                    ["Akun Piutang", f"{inv.ar_account_code} - {inv.ar_account_name}"],
                    ["Akun Penjualan", f"{inv.revenue_account_code} - {inv.revenue_account_name}"],
                ]
            )
        )

        section_title(story, "RINGKASAN")
        story.append(
            table_2col(
                [
                    ["Total", fmt_idr(total)],
                    ["Terbayar", fmt_idr(paid)],
                    ["Sisa Piutang", fmt_idr(remaining)],
                ]
            )
        )

        doc.build(story, onFirstPage=footer_canvas(), onLaterPages=footer_canvas())
        return send_file(
        tmp.name,
        as_attachment=False,  # IMPORTANT: tampilkan di browser
        download_name=f"invoice-{inv.invoice_no}.pdf",
        mimetype="application/pdf",
        )

from sqlalchemy import func

@bp.get("/admin/audit/unbalanced")
def audit_unbalanced_entries():
    # boleh kamu ganti jadi _require_access kalau mau semua user bisa lihat
    guard = _require_admin()
    if guard:
        return guard

    # pakai param ?to=YYYY-MM-DD (as-of)
    to_str = (request.args.get("to") or "").strip()
    if to_str:
        dto = _parse_ymd(to_str)
        if dto is None:
            flash("Format tanggal tidak valid.", "error")
            return redirect(url_for("main.dashboard"))
    else:
        dto = datetime.utcnow().date()

    to_dt_excl = datetime.combine(dto, datetime.min.time()) + timedelta(days=1)

    fk = _jl_entry_fk()

    rows = (
        db.session.query(
            JournalEntry.id,
            JournalEntry.date,
            JournalEntry.memo,
            func.coalesce(func.sum(JournalLine.debit), 0.0).label("td"),
            func.coalesce(func.sum(JournalLine.credit), 0.0).label("tc"),
        )
        .join(JournalLine, fk == JournalEntry.id)
        .filter(JournalEntry.date < to_dt_excl)
        .group_by(JournalEntry.id, JournalEntry.date, JournalEntry.memo)
        .having(func.abs(func.coalesce(func.sum(JournalLine.debit), 0.0) - func.coalesce(func.sum(JournalLine.credit), 0.0)) > 0.0001)
        .order_by(JournalEntry.date.asc(), JournalEntry.id.asc())
        .all()
    )

    # tampilkan ringkas aja via template sederhana (atau return text)
    return render_template("audit_unbalanced.html", rows=rows, dto=dto.strftime("%Y-%m-%d"))

# ============================================================
# EDIT / DELETE + REBUILD (STOK + JURNAL)
# ============================================================

from sqlalchemy import and_

def _delete_journal_entry(entry_id: int | None):
    if not entry_id:
        return
    # hapus lines dulu
    JournalLine.query.filter_by(entry_id=entry_id).delete()
    JournalEntry.query.filter_by(id=entry_id).delete()


def _recalc_purchase_paid_flags():
    """
    Set purchase.is_paid berdasarkan total pembayaran APayment per purchase.
    """
    purchases = Purchase.query.all()
    for p in purchases:
        if not p.id:
            continue
        total_paid = (
            db.session.query(db.func.coalesce(db.func.sum(APayment.amount), 0.0))
            .filter(APayment.purchase_id == p.id)
            .scalar()
            or 0.0
        )
        total = float(p.total_amount or 0)
        p.is_paid = bool(total_paid >= total and total > 0)


def _recalc_invoice_paid_fields():
    """
    Set invoice.paid_amount & status berdasarkan total pembayaran ARPayment per invoice.
    """
    invoices = SalesInvoice.query.all()
    for inv in invoices:
        total_paid = (
            db.session.query(db.func.coalesce(db.func.sum(ARPayment.amount), 0.0))
            .filter(ARPayment.invoice_id == inv.id)
            .scalar()
            or 0.0
        )
        inv.paid_amount = float(total_paid)
        total = float(inv.total_amount or 0)

        if total <= 0:
            inv.status = "unpaid"
        elif inv.paid_amount <= 0:
            inv.status = "unpaid"
        elif inv.paid_amount >= total:
            inv.status = "paid"
            inv.paid_amount = total  # clamp
        else:
            inv.status = "partial"


def _rebuild_inventory():
    """
    Rebuild stok & avg_cost semua item dari histori:
      + PurchaseItem (masuk)
      - StockUsage (keluar)
    Kita hitung avg_cost metode moving average (sesuai logika kamu sekarang).
    """
    items = Item.query.all()
    for it in items:
        it.stock_qty = 0.0
        it.avg_cost = 0.0

    # kumpulkan semua event pembelian & pemakaian per item
    # purchase: +qty, avg cost update
    purchase_rows = (
        db.session.query(PurchaseItem, Purchase)
        .join(Purchase, PurchaseItem.purchase_id == Purchase.id)
        .order_by(Purchase.date.asc(), Purchase.id.asc(), PurchaseItem.id.asc())
        .all()
    )

    usage_rows = (
        StockUsage.query.order_by(StockUsage.date.asc(), StockUsage.id.asc()).all()
    )

    # kita gabungkan event menjadi list (date, type, obj)
    events = []
    for pi, p in purchase_rows:
        events.append((p.date, 0, "purchase", pi))  # 0 supaya purchase duluan kalau tanggal sama
    for u in usage_rows:
        events.append((u.date, 1, "usage", u))      # 1 usage setelah purchase di hari sama
    events.sort(key=lambda x: (x[0] or datetime.min, x[1]))

    # map item_id -> item object
    item_map = {it.id: it for it in items}

    for _, _, etype, obj in events:
        if etype == "purchase":
            pi: PurchaseItem = obj
            it = item_map.get(pi.item_id)
            if not it:
                continue
            qty = float(pi.qty or 0)
            price = float(pi.price or 0)
            if qty <= 0:
                continue

            total_cost_existing = float(it.stock_qty or 0) * float(it.avg_cost or 0)
            total_cost_new = qty * price
            new_qty = float(it.stock_qty or 0) + qty
            it.avg_cost = (total_cost_existing + total_cost_new) / new_qty if new_qty > 0 else 0.0
            it.stock_qty = new_qty

        elif etype == "usage":
            u: StockUsage = obj
            it = item_map.get(u.item_id)
            if not it:
                continue
            qty = float(u.qty or 0)
            if qty <= 0:
                continue

            # keluar: kurangi qty, avg_cost tetap
            it.stock_qty = float(it.stock_qty or 0) - qty
            if it.stock_qty < 0:
                it.stock_qty = 0  # safeguard


def _rebuild_all_journals():
    """
    Hapus semua journal entries/lines lalu buat ulang berdasarkan semua transaksi.
    Ini yang bikin neraca balik balance setelah edit/hapus.
    """
    # Hapus semua dulu
    JournalLine.query.delete()
    JournalEntry.query.delete()

    # Reset FK jurnal di transaksi
    CashTransaction.query.update({CashTransaction.journal_entry_id: None})
    Purchase.query.update({Purchase.journal_entry_id: None})
    APayment.query.update({APayment.journal_entry_id: None})
    StockUsage.query.update({StockUsage.journal_entry_id: None})
    SalesInvoice.query.update({SalesInvoice.journal_entry_id: None})
    ARPayment.query.update({ARPayment.journal_entry_id: None})

    db.session.flush()

    # 1) CashTransaction
    txs = CashTransaction.query.order_by(CashTransaction.date.asc(), CashTransaction.id.asc()).all()
    for tx in txs:
        entry = _create_journal_for_cash(tx)
        tx.journal_entry_id = entry.id

    # 2) Purchase
    purchases = Purchase.query.order_by(Purchase.date.asc(), Purchase.id.asc()).all()
    for p in purchases:
        entry = _create_journal_for_purchase(p)
        p.journal_entry_id = entry.id

    # 3) AP Payment
    pays = APayment.query.order_by(APayment.date.asc(), APayment.id.asc()).all()
    for pay in pays:
        entry = _create_journal_for_ap_payment(pay)
        pay.journal_entry_id = entry.id

    # 4) Stock Usage
    usages = StockUsage.query.order_by(StockUsage.date.asc(), StockUsage.id.asc()).all()
    for u in usages:
        entry = _create_journal_for_stock_usage(u)
        u.journal_entry_id = entry.id

    # 5) Sales Invoice
    invoices = SalesInvoice.query.order_by(SalesInvoice.date.asc(), SalesInvoice.id.asc()).all()
    for inv in invoices:
        entry = _create_journal_for_invoice(inv)
        inv.journal_entry_id = entry.id

    # 6) AR Payment
    arps = ARPayment.query.order_by(ARPayment.date.asc(), ARPayment.id.asc()).all()
    for p in arps:
        inv = SalesInvoice.query.get(p.invoice_id) if p.invoice_id else None
        if not inv:
            continue
        entry = _create_journal_for_ar_payment(p, inv)
        p.journal_entry_id = entry.id


def _rebuild_everything():
    """
    Dipanggil setelah edit/hapus transaksi apa pun.
    """
    _rebuild_inventory()
    _recalc_purchase_paid_flags()
    _recalc_invoice_paid_fields()
    _rebuild_all_journals()
    db.session.commit()


# ============================================================
# AR PAYMENT - EDIT / DELETE
# ============================================================

@bp.route("/ar/payments/<int:pay_id>/edit", methods=["GET", "POST"])
def ar_payment_edit(pay_id: int):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    pay = ARPayment.query.get_or_404(pay_id)
    cash_accounts = Account.query.filter(Account.type == "Kas & Bank").order_by(Account.code.asc()).all()
    invoices = SalesInvoice.query.order_by(SalesInvoice.date.desc()).all()

    if request.method == "POST":
        date_str = (request.form.get("date") or "").strip()
        invoice_id = (request.form.get("invoice_id") or "").strip()
        cash_code = (request.form.get("cash_account") or "").strip()
        amount_str = (request.form.get("amount") or "").strip()
        memo = (request.form.get("memo") or "").strip()

        if not date_str or not invoice_id or not cash_code or not amount_str:
            flash("Field wajib belum lengkap.", "error")
            return redirect(url_for("main.ar_payment_edit", pay_id=pay_id))

        inv = SalesInvoice.query.get(int(invoice_id))
        if not inv:
            flash("Invoice tidak ditemukan.", "error")
            return redirect(url_for("main.ar_payment_edit", pay_id=pay_id))

        cash_acc = Account.query.filter_by(code=cash_code).first()
        if not cash_acc:
            flash("Akun kas/bank tidak valid.", "error")
            return redirect(url_for("main.ar_payment_edit", pay_id=pay_id))

        try:
            amt = float(amount_str)
            if amt <= 0:
                raise ValueError()
        except ValueError:
            flash("Nominal harus angka > 0.", "error")
            return redirect(url_for("main.ar_payment_edit", pay_id=pay_id))

        pay.date = _parse_date(date_str)
        pay.invoice_id = inv.id
        pay.invoice_no = inv.invoice_no
        pay.cash_account_code = cash_acc.code
        pay.cash_account_name = cash_acc.name
        pay.amount = amt
        pay.memo = memo or None

        db.session.commit()
        _rebuild_everything()

        flash("Pembayaran piutang diupdate.", "success")
        return redirect(url_for("main.ar_payment_home"))

    return render_template("ar_payment_edit.html", pay=pay, cash_accounts=cash_accounts, invoices=invoices)


@bp.post("/ar/payments/<int:pay_id>/delete")
def ar_payment_delete(pay_id: int):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.enter_code"))

    pay = ARPayment.query.get_or_404(pay_id)
    db.session.delete(pay)
    db.session.commit()
    _rebuild_everything()

    flash("Pembayaran piutang dihapus.", "success")
    return redirect(url_for("main.ar_payment_home"))
