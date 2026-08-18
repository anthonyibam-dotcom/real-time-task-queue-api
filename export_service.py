import csv
from pathlib import Path
from typing import Any


def demo_rows() -> list[dict[str, Any]]:
    return [
        {"student_id": "S-1001", "name": "Ada Lovelace", "average": 96.5, "status": "excellent"},
        {"student_id": "S-1002", "name": "Alan Turing", "average": 91.0, "status": "excellent"},
        {"student_id": "S-1003", "name": "Grace Hopper", "average": 88.5, "status": "good"},
    ]


def write_csv_report(output_path: Path, rows: list[dict[str, Any]]) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0]) if rows else ["student_id", "name", "average", "status"]
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def write_pdf_report(output_path: Path, rows: list[dict[str, Any]]) -> Path:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import inch
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    output_path.parent.mkdir(parents=True, exist_ok=True)
    document = SimpleDocTemplate(str(output_path), pagesize=letter, leftMargin=0.6 * inch, rightMargin=0.6 * inch)
    styles = getSampleStyleSheet()
    content = [Paragraph("Student Performance Report", styles["Title"]), Spacer(1, 0.2 * inch)]
    table_data = [["Student ID", "Name", "Average", "Status"]]
    table_data.extend([[row["student_id"], row["name"], str(row["average"]), row["status"]] for row in rows])
    table = Table(table_data, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#17324d")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#b8c7d1")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#eef4f7")]),
        ("PADDING", (0, 0), (-1, -1), 7),
    ]))
    content.append(table)
    document.build(content)
    return output_path
