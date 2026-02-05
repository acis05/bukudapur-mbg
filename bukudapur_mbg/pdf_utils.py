from __future__ import annotations

from io import BytesIO
from typing import Any, List, Optional, Sequence

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)

_styles = getSampleStyleSheet()

STYLE_TITLE = ParagraphStyle(
    "BD_Title",
    parent=_styles["Heading1"],
    fontSize=14,
    leading=18,
    spaceAfter=2,
)

STYLE_SUB = ParagraphStyle(
    "BD_Sub",
    parent=_styles["Normal"],
    fontSize=9,
    leading=12,
    textColor=colors.HexColor("#6b7280"),
    spaceAfter=8,
)

STYLE_META_L = ParagraphStyle(
    "BD_MetaL",
    parent=_styles["Normal"],
    fontSize=9,
    leading=12,
    alignment=TA_LEFT,
)

STYLE_META_R = ParagraphStyle(
    "BD_MetaR",
    parent=_styles["Normal"],
    fontSize=9,
    leading=12,
    alignment=TA_RIGHT,
)

STYLE_CELL = ParagraphStyle(
    "BD_Cell",
    parent=_styles["Normal"],
    fontSize=9,
    leading=12,
)

STYLE_CELL_MUTED = ParagraphStyle(
    "BD_CellMuted",
    parent=_styles["Normal"],
    fontSize=9,
    leading=12,
    textColor=colors.HexColor("#6b7280"),
)

GRID_COLOR = colors.HexColor("#e5e7eb")
HEADER_BG = colors.HexColor("#f3f4f6")
HEADER_LINE = colors.HexColor("#d1d5db")


def fmt_idr(x: Any) -> str:
    try:
        return f"Rp {float(x or 0):,.0f}"
    except Exception:
        return "Rp 0"


def pdf_doc(
    arg,
    filename: str | None = None,
    pagesize=A4,
    leftMargin=36,
    rightMargin=36,
    topMargin=36,
    bottomMargin=36,
    onFirstPage=None,
    onLaterPages=None,
):
    """
    Mode A (DocTemplate):
        doc = pdf_doc("output.pdf")
        doc.build(story, onFirstPage=..., onLaterPages=...)

    Mode B (bytes):
        pdf_bytes = pdf_doc(story)
    """
    # Mode B: arg adalah story
    if isinstance(arg, (list, tuple)):
        story = list(arg)
        buf = BytesIO()
        doc = SimpleDocTemplate(
            buf,
            pagesize=pagesize,
            leftMargin=leftMargin,
            rightMargin=rightMargin,
            topMargin=topMargin,
            bottomMargin=bottomMargin,
        )
        doc.build(story, onFirstPage=onFirstPage, onLaterPages=onLaterPages)
        return buf.getvalue()

    # Mode A: arg adalah path
    path = str(arg)
    return SimpleDocTemplate(
        path,
        pagesize=pagesize,
        leftMargin=leftMargin,
        rightMargin=rightMargin,
        topMargin=topMargin,
        bottomMargin=bottomMargin,
    )


def header_block(
    story: List[Any],
    title: str,
    subtitle: str = "",
    currency_text: str = "Mata Uang: Indonesian Rupiah",
    dapur_name: str = "Dapur MBG",
    right_text: str = "BukuDapur MBG",
):
    """
    Append header ke story (bukan return list).
    """
    # top row
    t = Table(
        [[
            Paragraph(f"<b>{dapur_name}</b>", STYLE_META_L),
            Paragraph(f"<b>{right_text}</b>", STYLE_META_R),
        ]],
        colWidths=[None, 55 * mm],
    )
    t.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
    ]))
    story.append(t)

    story.append(Paragraph(title, STYLE_TITLE))
    if subtitle:
        story.append(Paragraph(subtitle, STYLE_SUB))
    if currency_text:
        story.append(Paragraph(currency_text, STYLE_CELL_MUTED))
    story.append(Spacer(1, 10))


def section_title(story: List[Any], text: str):
    story.append(Paragraph(f"<b>{text}</b>", STYLE_META_L))
    story.append(Spacer(1, 6))


def subsection_title(story: List[Any], text: str):
    story.append(Paragraph(text, STYLE_CELL_MUTED))
    story.append(Spacer(1, 4))


def _cell(v: Any, font_size: int) -> Any:
    if isinstance(v, Paragraph):
        return v
    if v is None:
        v = ""
    return Paragraph(
        str(v),
        ParagraphStyle(
            "BD_CellDynamic",
            parent=STYLE_CELL,
            fontSize=font_size,
            leading=font_size + 3,
        ),
    )


def table_block(
    rows: Sequence[Sequence[Any]],
    col_widths: Optional[Sequence[float]] = None,
    header_rows: int = 1,
    font_size: int = 9,
    row_heights: Optional[Sequence[float]] = None,
    align_right_cols: Optional[Sequence[int]] = None,
) -> List[Any]:
    """
    Return list flowables supaya bisa: story += table_block(...)
    """
    data = [[_cell(v, font_size) for v in row] for row in rows]

    tbl = Table(data, colWidths=col_widths, rowHeights=row_heights, hAlign="LEFT")

    style_cmds = [
        ("GRID", (0, 0), (-1, -1), 0.25, GRID_COLOR),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]

    if header_rows and header_rows > 0:
        style_cmds += [
            ("BACKGROUND", (0, 0), (-1, header_rows - 1), HEADER_BG),
            ("TEXTCOLOR", (0, 0), (-1, header_rows - 1), colors.HexColor("#111827")),
            ("LINEBELOW", (0, header_rows - 1), (-1, header_rows - 1), 0.8, HEADER_LINE),
        ]

    if align_right_cols:
        for c in align_right_cols:
            style_cmds.append(("ALIGN", (c, 0), (c, -1), "RIGHT"))

    tbl.setStyle(TableStyle(style_cmds))

    return [tbl, Spacer(1, 8)]


def table_2col(rows, col_widths=None, header: bool = False, font_size: int = 9):
    """
    Return Table (bukan list), supaya aman dipakai: story.append(table_2col(...))
    """
    header_rows = 1 if header else 0
    flow = table_block(rows, col_widths=col_widths, header_rows=header_rows, font_size=font_size)
    return flow[0]


def table_3col(rows, col_widths=None, header: bool = True, font_size: int = 9, align_right_cols=None):
    """
    Return Table (bukan list), supaya aman dipakai: story.append(table_3col(...))
    """
    header_rows = 1 if header else 0
    flow = table_block(rows, col_widths=col_widths, header_rows=header_rows, font_size=font_size, align_right_cols=align_right_cols)
    return flow[0]


def footer_canvas():
    """
    Return callback function untuk doc.build(onFirstPage=..., onLaterPages=...)
    """
    def _cb(canvas, doc):
        # no-op footer (aman)
        return
    return _cb
