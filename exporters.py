"""Formatted data export: CSV, Excel (.xlsx), PDF report, PNG / JPG chart."""

from __future__ import annotations

import io
from datetime import datetime
from typing import List, Optional

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

PALETTE = ["#2563EB", "#F97316", "#10B981", "#A855F7", "#EF4444", "#0EA5E9", "#EAB308", "#64748B"]


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _naive(df: pd.DataFrame) -> pd.DataFrame:
    """Excel cannot store tz-aware datetimes -> drop tz (values stay local wall-time)."""
    out = df.copy()
    for col in out.columns:
        if isinstance(out[col].dtype, pd.DatetimeTZDtype):
            out[col] = out[col].dt.tz_localize(None)
    return out


def scales_differ(df: pd.DataFrame, factor: float = 20.0) -> bool:
    """True when sensors of one metric live on very different scales (e.g. BTC vs SOL)."""
    means = df.groupby("sensor_id")["value"].mean().abs()
    means = means[means > 0]
    return len(means) > 1 and (means.max() / means.min()) > factor


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Index every sensor to 100 at its first point in view."""
    out = df.sort_values("timestamp").copy()
    first = out.groupby("sensor_id")["value"].transform("first")
    out["value"] = np.where(first != 0, out["value"] / first * 100.0, np.nan)
    return out


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    """Per sensor/metric statistics."""
    if df.empty:
        return pd.DataFrame(columns=["sensor_id", "metric_type", "count", "mean", "min", "max", "std", "last"])
    g = df.sort_values("timestamp").groupby(["sensor_id", "metric_type"])["value"]
    out = g.agg(count="count", mean="mean", min="min", max="max", std="std", last="last").reset_index()
    return out.round(4)


def csv_bytes(df: pd.DataFrame) -> bytes:
    return _naive(df).to_csv(index=False).encode("utf-8")


# --------------------------------------------------------------------------- #
# chart image
# --------------------------------------------------------------------------- #
def chart_figure(df: pd.DataFrame, title: str = "Data export", max_panels: int = 4):
    """One panel per metric (max `max_panels`), one line per sensor."""
    metrics = list(df["metric_type"].unique())[:max_panels] if not df.empty else []
    n = max(1, len(metrics))
    fig, axes = plt.subplots(n, 1, figsize=(10, 2.8 * n + 0.8), squeeze=False)
    for ax, metric in zip(axes[:, 0], metrics or [None]):
        if metric is None:
            ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
            ax.set_axis_off()
            continue
        sub = df[df["metric_type"] == metric]
        indexed = scales_differ(sub)
        if indexed:
            sub = normalize(sub)
        for i, (sensor, grp) in enumerate(sub.groupby("sensor_id")):
            grp = grp.sort_values("timestamp")
            ax.plot(grp["timestamp"], grp["value"], color=PALETTE[i % len(PALETTE)],
                    linewidth=1.6, label=str(sensor), marker="o" if len(grp) < 40 else None, markersize=3)
        unit = sub["unit"].dropna().iloc[0] if "unit" in sub and sub["unit"].notna().any() else ""
        label = "index, start = 100" if indexed else unit
        ax.set_title(f"{metric}" + (f" ({label})" if label else ""), loc="left", fontsize=10, fontweight="bold")
        ax.grid(alpha=0.25)
        ax.spines[["top", "right"]].set_visible(False)
        tz = sub["timestamp"].dt.tz  # keep the display time zone instead of matplotlib's UTC default
        loc = mdates.AutoDateLocator(tz=tz)
        ax.xaxis.set_major_locator(loc)
        ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(loc, tz=tz))
        if sub["sensor_id"].nunique() > 1:
            ax.legend(fontsize=8, frameon=False, ncol=3)
    fig.suptitle(title, x=0.01, ha="left", fontsize=13, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    return fig


def image_bytes(df: pd.DataFrame, title: str, fmt: str = "png") -> bytes:
    fig = chart_figure(df, title)
    buf = io.BytesIO()
    fig.savefig(buf, format="jpeg" if fmt.lower() in ("jpg", "jpeg") else "png", dpi=150,
                facecolor="white")
    plt.close(fig)
    return buf.getvalue()


# --------------------------------------------------------------------------- #
# Excel
# --------------------------------------------------------------------------- #
def xlsx_bytes(df: pd.DataFrame, title: str = "Data export") -> bytes:
    data = _naive(df)
    summary = summarize(df)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter", datetime_format="yyyy-mm-dd hh:mm:ss") as xw:
        wb = xw.book
        head = wb.add_format({"bold": True, "font_color": "white", "bg_color": "#1E3A8A",
                              "border": 1, "align": "center", "valign": "vcenter"})
        title_fmt = wb.add_format({"bold": True, "font_size": 14})

        # --- Readings sheet
        data.to_excel(xw, sheet_name="Readings", index=False, startrow=2, header=False)
        ws = xw.sheets["Readings"]
        ws.write(0, 0, title, title_fmt)
        for c, name in enumerate(data.columns):
            ws.write(2, c, name, head)
            width = max(len(str(name)), *(len(str(v)) for v in data[name].head(200))) if len(data) else len(name)
            ws.set_column(c, c, min(max(width + 2, 10), 40))
        ws.freeze_panes(3, 0)
        if len(data):
            ws.autofilter(2, 0, 2 + len(data), len(data.columns) - 1)

        # --- Summary sheet
        summary.to_excel(xw, sheet_name="Summary", index=False, startrow=2, header=False)
        ws2 = xw.sheets["Summary"]
        ws2.write(0, 0, "Summary statistics per sensor / metric", title_fmt)
        for c, name in enumerate(summary.columns):
            ws2.write(2, c, name, head)
            ws2.set_column(c, c, 16)

        # --- Chart sheet (pivot of the most populated metric + native Excel chart)
        if len(data):
            metric = data["metric_type"].value_counts().idxmax()
            piv = (data[data["metric_type"] == metric]
                   .pivot_table(index="timestamp", columns="sensor_id", values="value", aggfunc="mean")
                   .tail(500).reset_index())
            piv.to_excel(xw, sheet_name="Chart", index=False)
            ws3 = xw.sheets["Chart"]
            ws3.set_column(0, 0, 20)
            chart = wb.add_chart({"type": "line"})
            for i in range(1, len(piv.columns)):
                chart.add_series({
                    "name": ["Chart", 0, i],
                    "categories": ["Chart", 1, 0, len(piv), 0],
                    "values": ["Chart", 1, i, len(piv), i],
                    "line": {"width": 1.75, "color": PALETTE[(i - 1) % len(PALETTE)]},
                })
            chart.set_title({"name": f"{metric} over time"})
            chart.set_x_axis({"date_axis": True, "num_format": "hh:mm", "name": "Time"})
            chart.set_size({"width": 860, "height": 380})
            ws3.insert_chart(1, len(piv.columns) + 1, chart)
    return buf.getvalue()


# --------------------------------------------------------------------------- #
# PDF
# --------------------------------------------------------------------------- #
def pdf_bytes(df: pd.DataFrame, title: str = "Data export", filters: Optional[List[str]] = None,
              max_rows: int = 40) -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    styles = getSampleStyleSheet()
    buf = io.BytesIO()

    def footer(canvas, doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.grey)
        canvas.drawString(1.5 * cm, 1 * cm, f"Generated {datetime.now():%Y-%m-%d %H:%M}")
        canvas.drawRightString(A4[0] - 1.5 * cm, 1 * cm, f"Page {doc.page}")
        canvas.restoreState()

    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=1.5 * cm, rightMargin=1.5 * cm,
                            topMargin=1.5 * cm, bottomMargin=1.8 * cm, title=title)
    flow = [Paragraph(title, styles["Title"])]
    meta = [f"Records: {len(df):,}"]
    if len(df):
        meta.append(f"Period: {df['timestamp'].min():%Y-%m-%d %H:%M} to {df['timestamp'].max():%Y-%m-%d %H:%M}")
    meta += filters or []
    flow += [Paragraph(" &nbsp;|&nbsp; ".join(meta), styles["Normal"]), Spacer(1, 0.4 * cm)]

    def table(frame: pd.DataFrame, col_widths=None, font=7.5):
        rows = [list(frame.columns)] + [[("" if pd.isna(v) else str(v)) for v in r] for r in frame.values.tolist()]
        t = Table(rows, colWidths=col_widths, repeatRows=1)
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1E3A8A")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), font),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F1F5F9")]),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CBD5E1")),
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ]))
        return t

    summary = summarize(df)
    flow.append(Paragraph("Summary statistics", styles["Heading2"]))
    flow.append(table(summary.head(25)) if len(summary) else Paragraph("No data for the selected filters.", styles["Normal"]))
    if len(summary) > 25:
        flow.append(Paragraph(f"... {len(summary) - 25} more sensor/metric rows omitted", styles["Italic"]))

    if len(df):
        flow += [Spacer(1, 0.4 * cm), Paragraph("Trend", styles["Heading2"])]
        png = image_bytes(df, title, "png")
        flow.append(Image(io.BytesIO(png), width=18 * cm, height=18 * cm * _aspect(df)))

        flow += [Spacer(1, 0.4 * cm), Paragraph(f"Latest {min(max_rows, len(df))} readings", styles["Heading2"])]
        cols = [c for c in ("timestamp", "sensor_id", "metric_type", "value", "unit") if c in df.columns]
        tail = df.sort_values("timestamp", ascending=False).head(max_rows)[cols].copy()
        tail["timestamp"] = tail["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
        tail["value"] = tail["value"].round(4)
        flow.append(table(tail))
        if len(df) > max_rows:
            flow.append(Paragraph(f"Showing {max_rows} of {len(df):,} rows - export Excel/CSV for the full dataset.",
                                  styles["Italic"]))
    doc.build(flow, onFirstPage=footer, onLaterPages=footer)
    return buf.getvalue()


def _aspect(df: pd.DataFrame, max_panels: int = 4) -> float:
    n = max(1, min(df["metric_type"].nunique(), max_panels))
    return (2.8 * n + 0.8) / 10.0
