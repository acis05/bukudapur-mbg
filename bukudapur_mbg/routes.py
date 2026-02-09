# =========================
# PART 1 / 4
# Core helpers & journal engine (FIXED)
# =========================

from __future__ import annotations
from datetime import datetime, timedelta, date
from flask import Blueprint, current_app, flash, redirect, render_template, request, send_file, session, url_for
from sqlalchemy import func
from io import BytesIO
import secrets

from . import db
from .models import (
    AccessCode, Account, Supplier, Item,
    CashTransaction, JournalEntry, JournalLine,
    Purchase, PurchaseItem, APayment,
    SalesInvoice, SalesInvoiceLine, ARPayment,
    StockUsage,
)

bp = Blueprint("main", __name__)

SESSION_KEY = "access_code"
ADMIN_SESSION_KEY = "admin_logged_in"


# ============================================================
# ACCESS & ADMIN
# ============================================================

def _require_access():
    code = session.get(SESSION_KEY)
    if not code:
        return None
    acc = AccessCode.query.filter_by(code=code).first()
    if not acc:
        return None
    if acc.mark_expired_if_needed():
        db.session.commit()
    return acc if acc.status != "expired" else None


def _require_admin():
    if not session.get(ADMIN_SESSION_KEY):
        flash("Admin login required", "error")
        return redirect(url_for("main.admin_login"))
    return None


# ============================================================
# DATE HELPERS
# ============================================================

def _parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


# ============================================================
# JOURNAL CORE (FIXED)
# ============================================================

def _set_scope(obj, acc):
    if acc and hasattr(obj, "access_code_id"):
        obj.access_code_id = acc.id


def _build_cash_lines(tx: CashTransaction):
    if tx.direction == "in":
        return [
            JournalLine(
                account_code=tx.cash_account_code,
                account_name=tx.cash_account_name,
                debit=tx.amount,
                credit=0,
            ),
            JournalLine(
                account_code=tx.counter_account_code,
                account_name=tx.counter_account_name,
                debit=0,
                credit=tx.amount,
            ),
        ]
    else:
        return [
            JournalLine(
                account_code=tx.counter_account_code,
                account_name=tx.counter_account_name,
                debit=tx.amount,
                credit=0,
            ),
            JournalLine(
                account_code=tx.cash_account_code,
                account_name=tx.cash_account_name,
                debit=0,
                credit=tx.amount,
            ),
        ]


def rebuild_journal_for_cash(acc, tx: CashTransaction):
    """
    SAFE journal rebuild:
    - Never delete entry before FK is cleared
    """
    old_entry = None
    if tx.journal_entry_id:
        old_entry = JournalEntry.query.get(tx.journal_entry_id)

    tx.journal_entry_id = None
    db.session.flush()

    if old_entry:
        db.session.delete(old_entry)
        db.session.flush()

    entry = JournalEntry(
        date=tx.date,
        memo=tx.memo,
        source="cash",
        source_id=tx.id,
    )
    _set_scope(entry, acc)

    entry.lines = _build_cash_lines(tx)
    for ln in entry.lines:
        _set_scope(ln, acc)

    db.session.add(entry)
    db.session.flush()

    tx.journal_entry_id = entry.id
    return entry


# ============================================================
# PURCHASE JOURNAL
# ============================================================

def create_journal_for_purchase(acc, purchase: Purchase):
    inventory = Account.query.filter_by(code="10051", access_code_id=acc.id).first()
    ap = Account.query.filter_by(code="20011", access_code_id=acc.id).first()
    if not inventory or not ap:
        raise Exception("Akun Persediaan / Hutang belum ada")

    entry = JournalEntry(
        date=purchase.date,
        memo=purchase.memo,
        source="purchase",
        source_id=purchase.id,
    )
    _set_scope(entry, acc)

    amt = float(purchase.total_amount or 0)

    entry.lines.append(JournalLine(
        account_code=inventory.code,
        account_name=inventory.name,
        debit=amt,
        credit=0,
    ))
    entry.lines.append(JournalLine(
        account_code=ap.code,
        account_name=ap.name,
        debit=0,
        credit=amt,
    ))

    for ln in entry.lines:
        _set_scope(ln, acc)

    db.session.add(entry)
    db.session.flush()
    return entry


# ============================================================
# AP PAYMENT JOURNAL
# ============================================================

def create_journal_for_ap_payment(acc, pay: APayment):
    ap = Account.query.filter_by(code="20011", access_code_id=acc.id).first()
    cash = Account.query.filter_by(code=pay.cash_account_code, access_code_id=acc.id).first()
    if not ap or not cash:
        raise Exception("Akun AP / Kas tidak ditemukan")

    entry = JournalEntry(
        date=pay.date,
        memo=pay.memo,
        source="ap_payment",
        source_id=pay.id,
    )
    _set_scope(entry, acc)

    entry.lines.append(JournalLine(
        account_code=ap.code,
        account_name=ap.name,
        debit=pay.amount,
        credit=0,
    ))
    entry.lines.append(JournalLine(
        account_code=cash.code,
        account_name=cash.name,
        debit=0,
        credit=pay.amount,
    ))

    for ln in entry.lines:
        _set_scope(ln, acc)

    db.session.add(entry)
    db.session.flush()
    return entry

# =========================
# PART 2 / 4
# Cash & Expenses routes (FIXED)
# =========================

from datetime import datetime
from flask import request, redirect, render_template, flash, url_for

from . import db
from .models import CashTransaction, Account
from .routes import bp, _require_access, rebuild_journal_for_cash


# ============================================================
# CASH HOME (CREATE)
# ============================================================

@bp.route("/cash", methods=["GET", "POST"])
def cash_home():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.login"))

    accounts = Account.query.filter_by(access_code_id=acc.id).all()

    if request.method == "POST":
        try:
            tx = CashTransaction(
                date=datetime.strptime(request.form["date"], "%Y-%m-%d"),
                memo=request.form.get("memo"),
                direction=request.form["direction"],  # in / out
                amount=float(request.form["amount"]),
                cash_account_code=request.form["cash_account_code"],
                cash_account_name=request.form["cash_account_name"],
                counter_account_code=request.form["counter_account_code"],
                counter_account_name=request.form["counter_account_name"],
                access_code_id=acc.id,
            )

            db.session.add(tx)
            db.session.flush()  # dapetin tx.id

            rebuild_journal_for_cash(acc, tx)

            db.session.commit()
            flash("Transaksi kas berhasil disimpan", "success")
            return redirect(url_for("main.cash_home"))

        except Exception as e:
            db.session.rollback()
            flash(f"Gagal simpan transaksi kas: {e}", "error")

    txs = (
        CashTransaction.query
        .filter_by(access_code_id=acc.id)
        .order_by(CashTransaction.date.desc())
        .all()
    )

    return render_template(
        "cash.html",
        accounts=accounts,
        txs=txs,
    )


# ============================================================
# CASH EDIT
# ============================================================

@bp.route("/cash/<int:tx_id>/edit", methods=["GET", "POST"])
def cash_edit(tx_id):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.login"))

    tx = CashTransaction.query.filter_by(id=tx_id, access_code_id=acc.id).first_or_404()
    accounts = Account.query.filter_by(access_code_id=acc.id).all()

    if request.method == "POST":
        try:
            tx.date = datetime.strptime(request.form["date"], "%Y-%m-%d")
            tx.memo = request.form.get("memo")
            tx.direction = request.form["direction"]
            tx.amount = float(request.form["amount"])
            tx.cash_account_code = request.form["cash_account_code"]
            tx.cash_account_name = request.form["cash_account_name"]
            tx.counter_account_code = request.form["counter_account_code"]
            tx.counter_account_name = request.form["counter_account_name"]

            db.session.flush()

            # 🔥 SAFE rebuild
            rebuild_journal_for_cash(acc, tx)

            db.session.commit()
            flash("Transaksi kas berhasil diupdate", "success")
            return redirect(url_for("main.cash_home"))

        except Exception as e:
            db.session.rollback()
            flash(f"Gagal update transaksi kas: {e}", "error")

    return render_template(
        "cash_edit.html",
        tx=tx,
        accounts=accounts,
    )


# ============================================================
# EXPENSES (ALIAS CASH OUT)
# ============================================================

@bp.route("/expenses", methods=["GET", "POST"])
def expenses_home():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.login"))

    accounts = Account.query.filter_by(access_code_id=acc.id).all()

    if request.method == "POST":
        try:
            tx = CashTransaction(
                date=datetime.strptime(request.form["date"], "%Y-%m-%d"),
                memo=request.form.get("memo"),
                direction="out",
                amount=float(request.form["amount"]),
                cash_account_code=request.form["cash_account_code"],
                cash_account_name=request.form["cash_account_name"],
                counter_account_code=request.form["expense_account_code"],
                counter_account_name=request.form["expense_account_name"],
                access_code_id=acc.id,
            )

            db.session.add(tx)
            db.session.flush()

            rebuild_journal_for_cash(acc, tx)

            db.session.commit()
            flash("Pengeluaran berhasil disimpan", "success")
            return redirect(url_for("main.expenses_home"))

        except Exception as e:
            db.session.rollback()
            flash(f"Gagal simpan pengeluaran: {e}", "error")

    txs = (
        CashTransaction.query
        .filter_by(access_code_id=acc.id, direction="out")
        .order_by(CashTransaction.date.desc())
        .all()
    )

    return render_template(
        "expenses.html",
        accounts=accounts,
        txs=txs,
    )


# ============================================================
# EXPENSE EDIT
# ============================================================

@bp.route("/expenses/<int:tx_id>/edit", methods=["GET", "POST"])
def expense_edit(tx_id):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.login"))

    tx = CashTransaction.query.filter_by(id=tx_id, access_code_id=acc.id).first_or_404()
    accounts = Account.query.filter_by(access_code_id=acc.id).all()

    if request.method == "POST":
        try:
            tx.date = datetime.strptime(request.form["date"], "%Y-%m-%d")
            tx.memo = request.form.get("memo")
            tx.amount = float(request.form["amount"])
            tx.cash_account_code = request.form["cash_account_code"]
            tx.cash_account_name = request.form["cash_account_name"]
            tx.counter_account_code = request.form["expense_account_code"]
            tx.counter_account_name = request.form["expense_account_name"]

            db.session.flush()

            # 🔥 SAFE rebuild
            rebuild_journal_for_cash(acc, tx)

            db.session.commit()
            flash("Pengeluaran berhasil diupdate", "success")
            return redirect(url_for("main.expenses_home"))

        except Exception as e:
            db.session.rollback()
            flash(f"Gagal update pengeluaran: {e}", "error")

    return render_template(
        "expenses_edit.html",
        tx=tx,
        accounts=accounts,
    )

# =========================
# HELPER: REBUILD PURCHASE JOURNAL
# =========================

def rebuild_journal_for_purchase(acc, purchase):
    """
    Aman untuk CREATE & EDIT
    """
    old_entry = purchase.journal_entry

    # 1️⃣ putus FK dulu
    purchase.journal_entry = None
    purchase.journal_entry_id = None
    db.session.flush()

    # 2️⃣ hapus journal lama
    if old_entry:
        db.session.delete(old_entry)
        db.session.flush()

    # 3️⃣ buat journal baru
    entry = JournalEntry(
        date=purchase.date,
        memo=purchase.memo,
        source="purchase",
        source_id=purchase.id,
    )
    _set_entry_scope(entry, acc)
    db.session.add(entry)
    db.session.flush()

    # 4️⃣ posting debit / kredit
    # Debit: Persediaan / Beban
    entry.lines.append(JournalLine(
        account_code=purchase.expense_account_code,
        account_name=purchase.expense_account_name,
        debit=purchase.amount,
        credit=0,
    ))

    # Kredit: Hutang Usaha
    entry.lines.append(JournalLine(
        account_code=purchase.ap_account_code,
        account_name=purchase.ap_account_name,
        debit=0,
        credit=purchase.amount,
    ))

    db.session.flush()

    # 5️⃣ link balik
    purchase.journal_entry_id = entry.id
    db.session.flush()

    return entry

@bp.route("/purchases", methods=["GET", "POST"])
def purchase_home():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.login"))

    if request.method == "POST":
        try:
            purchase = Purchase(
                date=datetime.strptime(request.form["date"], "%Y-%m-%d"),
                memo=request.form.get("memo"),
                amount=float(request.form["amount"]),
                expense_account_code=request.form["expense_account_code"],
                expense_account_name=request.form["expense_account_name"],
                ap_account_code=request.form["ap_account_code"],
                ap_account_name=request.form["ap_account_name"],
                access_code_id=acc.id,
            )

            db.session.add(purchase)
            db.session.flush()

            rebuild_journal_for_purchase(acc, purchase)

            db.session.commit()
            flash("Pembelian berhasil disimpan", "success")
            return redirect(url_for("main.purchase_home"))

        except Exception as e:
            db.session.rollback()
            flash(f"Gagal simpan pembelian: {e}", "error")

    purchases = Purchase.query.filter_by(
        access_code_id=acc.id
    ).order_by(Purchase.date.desc()).all()

    return render_template("purchases.html", purchases=purchases)

@bp.route("/purchases/<int:pid>/edit", methods=["GET", "POST"])
def purchase_edit(pid):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.login"))

    purchase = Purchase.query.filter_by(
        id=pid, access_code_id=acc.id
    ).first_or_404()

    if request.method == "POST":
        try:
            purchase.date = datetime.strptime(request.form["date"], "%Y-%m-%d")
            purchase.memo = request.form.get("memo")
            purchase.amount = float(request.form["amount"])
            purchase.expense_account_code = request.form["expense_account_code"]
            purchase.expense_account_name = request.form["expense_account_name"]
            purchase.ap_account_code = request.form["ap_account_code"]
            purchase.ap_account_name = request.form["ap_account_name"]

            db.session.flush()

            rebuild_journal_for_purchase(acc, purchase)

            db.session.commit()
            flash("Pembelian berhasil diupdate", "success")
            return redirect(url_for("main.purchase_home"))

        except Exception as e:
            db.session.rollback()
            flash(f"Gagal update pembelian: {e}", "error")

    return render_template("purchase_edit.html", purchase=purchase)

@bp.route("/ap-payments", methods=["GET", "POST"])
def ap_payment_home():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.login"))

    if request.method == "POST":
        try:
            pay = APPayment(
                date=datetime.strptime(request.form["date"], "%Y-%m-%d"),
                memo=request.form.get("memo"),
                amount=float(request.form["amount"]),
                cash_account_code=request.form["cash_account_code"],
                cash_account_name=request.form["cash_account_name"],
                ap_account_code=request.form["ap_account_code"],
                ap_account_name=request.form["ap_account_name"],
                access_code_id=acc.id,
            )

            db.session.add(pay)
            db.session.flush()

            rebuild_journal_for_ap_payment(acc, pay)

            db.session.commit()
            flash("Pembayaran hutang berhasil", "success")
            return redirect(url_for("main.ap_payment_home"))

        except Exception as e:
            db.session.rollback()
            flash(f"Gagal simpan pembayaran hutang: {e}", "error")

    payments = APPayment.query.filter_by(
        access_code_id=acc.id
    ).order_by(APPayment.date.desc()).all()

    return render_template("ap_payments.html", payments=payments)

@bp.route("/ap-payments/<int:pid>/edit", methods=["GET", "POST"])
def ap_payment_edit(pid):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.login"))

    pay = APPayment.query.filter_by(
        id=pid, access_code_id=acc.id
    ).first_or_404()

    if request.method == "POST":
        try:
            pay.date = datetime.strptime(request.form["date"], "%Y-%m-%d")
            pay.memo = request.form.get("memo")
            pay.amount = float(request.form["amount"])
            pay.cash_account_code = request.form["cash_account_code"]
            pay.cash_account_name = request.form["cash_account_name"]
            pay.ap_account_code = request.form["ap_account_code"]
            pay.ap_account_name = request.form["ap_account_name"]

            db.session.flush()

            rebuild_journal_for_ap_payment(acc, pay)

            db.session.commit()
            flash("Pembayaran hutang berhasil diupdate", "success")
            return redirect(url_for("main.ap_payment_home"))

        except Exception as e:
            db.session.rollback()
            flash(f"Gagal update pembayaran hutang: {e}", "error")

    return render_template("ap_payment_edit.html", pay=pay)

# =========================
# HELPER: REBUILD SALES INVOICE JOURNAL
# =========================
def rebuild_journal_for_invoice(acc, inv):
    old_entry = inv.journal_entry

    # 1️⃣ putus FK
    inv.journal_entry = None
    inv.journal_entry_id = None
    db.session.flush()

    # 2️⃣ hapus journal lama
    if old_entry:
        db.session.delete(old_entry)
        db.session.flush()

    # 3️⃣ journal baru
    entry = JournalEntry(
        date=inv.date,
        memo=inv.memo,
        source="invoice",
        source_id=inv.id,
    )
    _set_entry_scope(entry, acc)
    db.session.add(entry)
    db.session.flush()

    # Debit: Piutang Usaha
    entry.lines.append(JournalLine(
        account_code=inv.ar_account_code,
        account_name=inv.ar_account_name,
        debit=inv.total_amount,
        credit=0,
    ))

    # Kredit: Pendapatan
    entry.lines.append(JournalLine(
        account_code=inv.revenue_account_code,
        account_name=inv.revenue_account_name,
        debit=0,
        credit=inv.total_amount,
    ))

    db.session.flush()

    # 4️⃣ link balik
    inv.journal_entry_id = entry.id
    db.session.flush()

    return entry

@bp.route("/invoices", methods=["GET", "POST"])
def invoice_home():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.login"))

    if request.method == "POST":
        try:
            inv = SalesInvoice(
                date=datetime.strptime(request.form["date"], "%Y-%m-%d"),
                memo=request.form.get("memo"),
                total_amount=float(request.form["total_amount"]),
                ar_account_code=request.form["ar_account_code"],
                ar_account_name=request.form["ar_account_name"],
                revenue_account_code=request.form["revenue_account_code"],
                revenue_account_name=request.form["revenue_account_name"],
                access_code_id=acc.id,
            )

            db.session.add(inv)
            db.session.flush()

            rebuild_journal_for_invoice(acc, inv)

            db.session.commit()
            flash("Invoice berhasil disimpan", "success")
            return redirect(url_for("main.invoice_home"))

        except Exception as e:
            db.session.rollback()
            flash(f"Gagal simpan invoice: {e}", "error")

    invoices = SalesInvoice.query.filter_by(
        access_code_id=acc.id
    ).order_by(SalesInvoice.date.desc()).all()

    return render_template("invoices.html", invoices=invoices)

@bp.route("/invoices/<int:inv_id>/edit", methods=["GET", "POST"])
def invoice_edit(inv_id):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.login"))

    inv = SalesInvoice.query.filter_by(
        id=inv_id, access_code_id=acc.id
    ).first_or_404()

    if request.method == "POST":
        try:
            inv.date = datetime.strptime(request.form["date"], "%Y-%m-%d")
            inv.memo = request.form.get("memo")
            inv.total_amount = float(request.form["total_amount"])
            inv.ar_account_code = request.form["ar_account_code"]
            inv.ar_account_name = request.form["ar_account_name"]
            inv.revenue_account_code = request.form["revenue_account_code"]
            inv.revenue_account_name = request.form["revenue_account_name"]

            db.session.flush()

            rebuild_journal_for_invoice(acc, inv)

            db.session.commit()
            flash("Invoice berhasil diupdate", "success")
            return redirect(url_for("main.invoice_home"))

        except Exception as e:
            db.session.rollback()
            flash(f"Gagal update invoice: {e}", "error")

    return render_template("invoice_edit.html", invoice=inv)

# =========================
# HELPER: REBUILD AR PAYMENT JOURNAL
# =========================
def rebuild_journal_for_ar_payment(acc, pay):
    old_entry = pay.journal_entry

    pay.journal_entry = None
    pay.journal_entry_id = None
    db.session.flush()

    if old_entry:
        db.session.delete(old_entry)
        db.session.flush()

    entry = JournalEntry(
        date=pay.date,
        memo=pay.memo,
        source="ar_payment",
        source_id=pay.id,
    )
    _set_entry_scope(entry, acc)
    db.session.add(entry)
    db.session.flush()

    # Debit: Kas
    entry.lines.append(JournalLine(
        account_code=pay.cash_account_code,
        account_name=pay.cash_account_name,
        debit=pay.amount,
        credit=0,
    ))

    # Kredit: Piutang
    entry.lines.append(JournalLine(
        account_code=pay.ar_account_code,
        account_name=pay.ar_account_name,
        debit=0,
        credit=pay.amount,
    ))

    db.session.flush()
    pay.journal_entry_id = entry.id
    db.session.flush()

    return entry

@bp.route("/ar-payments", methods=["GET", "POST"])
def ar_payment_home():
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.login"))

    if request.method == "POST":
        try:
            pay = ARPayment(
                date=datetime.strptime(request.form["date"], "%Y-%m-%d"),
                memo=request.form.get("memo"),
                amount=float(request.form["amount"]),
                cash_account_code=request.form["cash_account_code"],
                cash_account_name=request.form["cash_account_name"],
                ar_account_code=request.form["ar_account_code"],
                ar_account_name=request.form["ar_account_name"],
                access_code_id=acc.id,
            )

            db.session.add(pay)
            db.session.flush()

            rebuild_journal_for_ar_payment(acc, pay)

            db.session.commit()
            flash("Pembayaran piutang berhasil", "success")
            return redirect(url_for("main.ar_payment_home"))

        except Exception as e:
            db.session.rollback()
            flash(f"Gagal simpan pembayaran piutang: {e}", "error")

    payments = ARPayment.query.filter_by(
        access_code_id=acc.id
    ).order_by(ARPayment.date.desc()).all()

    return render_template("ar_payments.html", payments=payments)

@bp.route("/ar-payments/<int:pid>/edit", methods=["GET", "POST"])
def ar_payment_edit(pid):
    acc = _require_access()
    if not acc:
        return redirect(url_for("main.login"))

    pay = ARPayment.query.filter_by(
        id=pid, access_code_id=acc.id
    ).first_or_404()

    if request.method == "POST":
        try:
            pay.date = datetime.strptime(request.form["date"], "%Y-%m-%d")
            pay.memo = request.form.get("memo")
            pay.amount = float(request.form["amount"])
            pay.cash_account_code = request.form["cash_account_code"]
            pay.cash_account_name = request.form["cash_account_name"]
            pay.ar_account_code = request.form["ar_account_code"]
            pay.ar_account_name = request.form["ar_account_name"]

            db.session.flush()

            rebuild_journal_for_ar_payment(acc, pay)

            db.session.commit()
            flash("Pembayaran piutang berhasil diupdate", "success")
            return redirect(url_for("main.ar_payment_home"))

        except Exception as e:
            db.session.rollback()
            flash(f"Gagal update pembayaran piutang: {e}", "error")

    return render_template("ar_payment_edit.html", pay=pay)
