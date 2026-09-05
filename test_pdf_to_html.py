import fitz

pdf_path = r"data/court/cnrorders/cmis/orders/HPHC010008182026_1_2026-08-27.pdf"
html_path = "test_judgment.html"

doc = fitz.open(pdf_path)

with open(html_path, "w", encoding="utf-8") as f:
    f.write("""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Judgment</title>
    </head>
    <body>
    """)

    for page in doc:
        f.write(page.get_text("html"))

    f.write("""
    </body>
    </html>
    """)

doc.close()

print(f"Converted {pdf_path}")
print(f"Saved HTML to {html_path}")
