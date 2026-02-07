from __future__ import annotations

from datetime import datetime
from . import db


# ============================================================
# ACCESS / TENANT
# ============================================================
class AccessCode(db.Model):
    __tablename__ = "access_codes"

    id = db.Column(db.Integer, primary_key=True)

    # Kode akses yang dipakai tim dapur
    code = db.Column(db.String(24), unique=True, nullable=False, index=True)

    # Nama dapur (opsional)
    dapur_name = db.Column(db.String(120), nullable=True)

    # trial / active / expired
    status = db.Column(db.String(16), nullable=False, default="trial")

    # Masa berlaku
    start_at = db.Column(db.DateTime, nullable=False)
    expires_at = db.Column(db.DateTime, nullable=False)

    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    # Relationships (opsional, tapi membantu)
    accounts = db.relationship("Account", backref="access", lazy=True)
    suppliers = db.relationship("Supplier", backref="access", lazy=True)
    items = db.relationship("Item", backref="access", lazy=True)

    def is_expired(self) -> bool:
        return datetime.utcnow() > self.expires_at

    def mark_expired_if_needed(self) -> bool:
        """Return True jika status berubah jadi expired."""
        if self.is_expired() and self.status != "expired":
            self.status = "expired"
            return True
        return False


# ============================================================
# MASTER DATA (per dapur)
# ============================================================
class Account(db.Model):
    __tablename__ = "accounts"

    id = db.Column(db.Integer, primary_key=True)

    access_code_id = db.Column(
        db.Integer, db.ForeignKey("access_codes.id"), nullable=False, index=True
    )

    # contoh: 1010, 5010
    code = db.Column(db.String(10), nullable=False, index=True)
    name = db.Column(db.String(120), nullable=False)

    # contoh type yang kamu pakai:
    # "Kas & Bank", "Akun Piutang", "Akun Hutang", "Pendapatan", "Pendapatan Lain",
    # "HPP", "Beban", "Beban Lain", dll
    type = db.Column(db.String(30), nullable=False)

    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        db.UniqueConstraint("access_code_id", "code", name="uq_accounts_tenant_code"),
    )


class Supplier(db.Model):
    __tablename__ = "suppliers"

    id = db.Column(db.Integer, primary_key=True)

    access_code_id = db.Column(
        db.Integer, db.ForeignKey("access_codes.id"), nullable=False, index=True
    )

    name = db.Column(db.String(120), nullable=False, index=True)
    phone = db.Column(db.String(40), nullable=True)
    address = db.Column(db.String(255), nullable=True)

    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        db.UniqueConstraint("access_code_id", "name", name="uq_suppliers_tenant_name"),
    )


class Item(db.Model):
    __tablename__ = "items"

    id = db.Column(db.Integer, primary_key=True)

    access_code_id = db.Column(
        db.Integer, db.ForeignKey("access_codes.id"), nullable=False, index=True
    )

    name = db.Column(db.String(120), nullable=False, index=True)
    category = db.Column(db.String(80), nullable=True)

    # contoh: kg, liter, pcs
    unit = db.Column(db.String(20), nullable=False, default="pcs")

    min_stock = db.Column(db.Float, nullable=False, default=0)

    # stok & nilai sederhana untuk MVP
    stock_qty = db.Column(db.Float, nullable=False, default=0)
    avg_cost = db.Column(db.Float, nullable=False, default=0)

    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        db.UniqueConstraint("access_code_id", "name", name="uq_items_tenant_name"),
    )


# ============================================================
# JOURNAL
# ============================================================
class JournalEntry(db.Model):
    __tablename__ = "journal_entries"

    id = db.Column(db.Integer, primary_key=True)

    access_code_id = db.Column(
        db.Integer, db.ForeignKey("access_codes.id"), nullable=False, index=True
    )

    date = db.Column(db.DateTime, nullable=False)
    memo = db.Column(db.String(255), nullable=True)

    # sumber transaksi (kas/pembelian/pemakaian), untuk tracking MVP
    source = db.Column(db.String(30), nullable=False, default="manual")
    source_id = db.Column(db.Integer, nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    lines = db.relationship(
        "JournalLine", backref="entry", cascade="all, delete-orphan", lazy=True
    )


class JournalLine(db.Model):
    __tablename__ = "journal_lines"

    id = db.Column(db.Integer, primary_key=True)

    access_code_id = db.Column(
        db.Integer, db.ForeignKey("access_codes.id"), nullable=False, index=True
    )

    entry_id = db.Column(db.Integer, db.ForeignKey("journal_entries.id"), nullable=False)

    account_code = db.Column(db.String(10), nullable=False, index=True)
    account_name = db.Column(db.String(120), nullable=False)

    debit = db.Column(db.Float, nullable=False, default=0)
    credit = db.Column(db.Float, nullable=False, default=0)

    __table_args__ = (
        db.Index("ix_journal_lines_tenant_account", "access_code_id", "account_code"),
    )


# ============================================================
# CASH TRANSACTION
# ============================================================
class CashTransaction(db.Model):
    __tablename__ = "cash_transactions"

    id = db.Column(db.Integer, primary_key=True)

    access_code_id = db.Column(
        db.Integer, db.ForeignKey("access_codes.id"), nullable=False, index=True
    )

    date = db.Column(db.DateTime, nullable=False)
    # in / out
    direction = db.Column(db.String(5), nullable=False)

    cash_account_code = db.Column(db.String(10), nullable=False)  # Kas / Bank
    cash_account_name = db.Column(db.String(120), nullable=False)

    counter_account_code = db.Column(db.String(10), nullable=False)  # lawan transaksi
    counter_account_name = db.Column(db.String(120), nullable=False)

    amount = db.Column(db.Float, nullable=False)
    memo = db.Column(db.String(255), nullable=True)

    journal_entry_id = db.Column(
        db.Integer, db.ForeignKey("journal_entries.id"), nullable=True
    )

    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


# ============================================================
# PURCHASE + AP PAYMENT
# ============================================================
class Purchase(db.Model):
    __tablename__ = "purchases"

    id = db.Column(db.Integer, primary_key=True)

    access_code_id = db.Column(
        db.Integer, db.ForeignKey("access_codes.id"), nullable=False, index=True
    )

    date = db.Column(db.DateTime, nullable=False)

    supplier_id = db.Column(db.Integer, db.ForeignKey("suppliers.id"), nullable=True)
    supplier_name = db.Column(db.String(120), nullable=True)

    total_amount = db.Column(db.Float, nullable=False)
    is_paid = db.Column(db.Boolean, default=False, nullable=False)

    journal_entry_id = db.Column(
        db.Integer, db.ForeignKey("journal_entries.id"), nullable=True
    )

    memo = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    items = db.relationship(
        "PurchaseItem", backref="purchase", cascade="all, delete-orphan", lazy=True
    )


class PurchaseItem(db.Model):
    __tablename__ = "purchase_items"

    id = db.Column(db.Integer, primary_key=True)

    access_code_id = db.Column(
        db.Integer, db.ForeignKey("access_codes.id"), nullable=False, index=True
    )

    purchase_id = db.Column(db.Integer, db.ForeignKey("purchases.id"), nullable=False)

    item_id = db.Column(db.Integer, db.ForeignKey("items.id"), nullable=False)
    item_name = db.Column(db.String(120), nullable=False)

    qty = db.Column(db.Float, nullable=False)
    price = db.Column(db.Float, nullable=False)
    subtotal = db.Column(db.Float, nullable=False)


class APayment(db.Model):
    __tablename__ = "ap_payments"

    id = db.Column(db.Integer, primary_key=True)

    access_code_id = db.Column(
        db.Integer, db.ForeignKey("access_codes.id"), nullable=False, index=True
    )

    date = db.Column(db.DateTime, nullable=False)

    purchase_id = db.Column(db.Integer, db.ForeignKey("purchases.id"), nullable=True)
    supplier_name = db.Column(db.String(120), nullable=True)

    cash_account_code = db.Column(db.String(10), nullable=False)
    cash_account_name = db.Column(db.String(120), nullable=False)

    amount = db.Column(db.Float, nullable=False)
    memo = db.Column(db.String(255), nullable=True)

    journal_entry_id = db.Column(
        db.Integer, db.ForeignKey("journal_entries.id"), nullable=True
    )
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


# ============================================================
# SALES INVOICE + AR PAYMENT
# ============================================================
class SalesInvoice(db.Model):
    __tablename__ = "sales_invoices"

    id = db.Column(db.Integer, primary_key=True)

    access_code_id = db.Column(
        db.Integer, db.ForeignKey("access_codes.id"), nullable=False, index=True
    )

    date = db.Column(db.DateTime, nullable=False)

    # invoice no harus unik per dapur (bukan global)
    invoice_no = db.Column(db.String(50), nullable=False)

    customer_name = db.Column(db.String(120), nullable=False)
    customer_phone = db.Column(db.String(50))
    notes = db.Column(db.String(255))

    ar_account_code = db.Column(db.String(20), nullable=False)  # Piutang
    ar_account_name = db.Column(db.String(120), nullable=False)

    revenue_account_code = db.Column(db.String(20), nullable=False)  # Pendapatan
    revenue_account_name = db.Column(db.String(120), nullable=False)

    total_amount = db.Column(db.Float, nullable=False, default=0)

    status = db.Column(db.String(20), nullable=False, default="unpaid")  # unpaid/partial/paid
    paid_amount = db.Column(db.Float, nullable=False, default=0)

    journal_entry_id = db.Column(db.Integer, db.ForeignKey("journal_entries.id"))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint(
            "access_code_id", "invoice_no", name="uq_sales_invoices_tenant_invoice_no"
        ),
    )


class SalesInvoiceLine(db.Model):
    __tablename__ = "sales_invoice_lines"

    id = db.Column(db.Integer, primary_key=True)

    access_code_id = db.Column(
        db.Integer, db.ForeignKey("access_codes.id"), nullable=False, index=True
    )

    invoice_id = db.Column(db.Integer, db.ForeignKey("sales_invoices.id"), nullable=False)

    description = db.Column(db.String(200), nullable=False)
    qty = db.Column(db.Float, nullable=False, default=1)
    unit = db.Column(db.String(30))
    price = db.Column(db.Float, nullable=False, default=0)
    amount = db.Column(db.Float, nullable=False, default=0)

    invoice = db.relationship(
        "SalesInvoice",
        backref=db.backref("lines", lazy=True, cascade="all, delete-orphan"),
    )


class ARPayment(db.Model):
    __tablename__ = "ar_payments"

    id = db.Column(db.Integer, primary_key=True)

    access_code_id = db.Column(
        db.Integer, db.ForeignKey("access_codes.id"), nullable=False, index=True
    )

    date = db.Column(db.DateTime, nullable=False)

    invoice_id = db.Column(db.Integer, db.ForeignKey("sales_invoices.id"), nullable=False)
    invoice_no = db.Column(db.String(50), nullable=False)

    cash_account_code = db.Column(db.String(20), nullable=False)
    cash_account_name = db.Column(db.String(120), nullable=False)

    amount = db.Column(db.Float, nullable=False, default=0)
    memo = db.Column(db.String(255))

    journal_entry_id = db.Column(db.Integer, db.ForeignKey("journal_entries.id"))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


# ============================================================
# STOCK USAGE (HPP)
# ============================================================
class StockUsage(db.Model):
    __tablename__ = "stock_usages"

    id = db.Column(db.Integer, primary_key=True)

    access_code_id = db.Column(
        db.Integer, db.ForeignKey("access_codes.id"), nullable=False, index=True
    )

    date = db.Column(db.DateTime, nullable=False)

    item_id = db.Column(db.Integer, db.ForeignKey("items.id"), nullable=False)
    item_name = db.Column(db.String(120), nullable=False)

    qty = db.Column(db.Float, nullable=False)
    unit_cost = db.Column(db.Float, nullable=False)
    total_cost = db.Column(db.Float, nullable=False)

    hpp_account_code = db.Column(db.String(10), nullable=False)
    hpp_account_name = db.Column(db.String(120), nullable=False)

    memo = db.Column(db.String(255), nullable=True)

    journal_entry_id = db.Column(
        db.Integer, db.ForeignKey("journal_entries.id"), nullable=True
    )
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
