# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Generate Large PDF for Parsing Tests
# MAGIC This notebook generates a 1,010-page PDF with varied, realistic content for testing document parsing pipelines.

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install fpdf2 -q

# COMMAND ----------

# DBTITLE 1,Create Volume
# MAGIC %md
# MAGIC ## Create Unity Catalog Volume
# MAGIC Create the target volume for storing the test PDF.

# COMMAND ----------

# DBTITLE 1,Create Volume If Not Exists
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS dennis_schultz.sharepoint_testing.test_documents

# COMMAND ----------

# DBTITLE 1,PDF Generation Section
# MAGIC %md
# MAGIC ## Generate 1,010-Page PDF
# MAGIC The PDF includes:
# MAGIC - Page numbers on every page
# MAGIC - Section headings every 10 pages
# MAGIC - Paragraphs of lorem ipsum text
# MAGIC - Tables of dummy data every 50 pages

# COMMAND ----------

# DBTITLE 1,Generate PDF Document
from fpdf import FPDF
import random
import string

# Lorem ipsum paragraphs for varied content
LOREM_PARAGRAPHS = [
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.",
    "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Curabitur pretium tincidunt lacus. Nulla gravida orci a odio. Nullam varius, turpis et commodo pharetra, est eros bibendum elit, nec luctus magna felis sollicitudin mauris.",
    "Integer in mauris eu nibh euismod gravida. Duis ac tellus et risus vulputate vehicula. Donec lobortis risus a elit. Etiam tempor. Ut ullamcorper, ligula ut dictum pharetra, nisi nunc fringilla magna, in commodo elit erat sit amet enim. Fusce feugiat malesuada odio.",
    "Morbi nunc odio, gravida at, cursus nec, luctus a, lorem. Maecenas tristique orci ac sem. Duis ultricies pharetra magna. Donec accumsan malesuada orci. Donec sit amet eros. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris fermentum dictum magna.",
    "Sed laoreet aliquam leo. Ut tellus dolor, dapibus eget, elementum vel, cursus eleifend, elit. Aenean auctor wisi et urna. Aliquam erat volutpat. Duis ac turpis. Integer rutrum ante eu lacus. Vestibulum libero nisl, porta vel, scelerisque eget, malesuada at, neque.",
]

SECTION_TITLES = [
    "Executive Summary", "Introduction", "Background", "Methodology",
    "Data Collection", "Analysis Framework", "Results Overview",
    "Statistical Findings", "Discussion", "Key Insights",
    "Market Analysis", "Competitive Landscape", "Risk Assessment",
    "Financial Projections", "Implementation Plan", "Technical Architecture",
    "Performance Metrics", "Quality Assurance", "Compliance Review",
    "Stakeholder Analysis", "Resource Allocation", "Timeline & Milestones",
    "Budget Summary", "Recommendations", "Conclusion",
    "Appendix A: Raw Data", "Appendix B: Methodology Details",
    "Appendix C: Supporting Charts", "Appendix D: References",
    "Appendix E: Glossary",
]


def add_table(pdf):
    """Add a table of dummy data to the PDF."""
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 8, "Data Summary Table", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    # Table header
    pdf.set_font("Helvetica", "B", 9)
    col_widths = [30, 45, 35, 40, 40]
    headers = ["ID", "Category", "Value", "Date", "Status"]
    for i, header in enumerate(headers):
        pdf.cell(col_widths[i], 7, header, border=1)
    pdf.ln()

    # Table rows
    pdf.set_font("Helvetica", "", 9)
    categories = ["Finance", "Operations", "Marketing", "Engineering", "Sales"]
    statuses = ["Active", "Pending", "Completed", "In Review", "Archived"]
    for row in range(12):
        pdf.cell(col_widths[0], 6, f"REC-{row+1:04d}", border=1)
        pdf.cell(col_widths[1], 6, random.choice(categories), border=1)
        pdf.cell(col_widths[2], 6, f"${random.randint(1000, 99999):,}", border=1)
        pdf.cell(col_widths[3], 6, f"2026-{random.randint(1,12):02d}-{random.randint(1,28):02d}", border=1)
        pdf.cell(col_widths[4], 6, random.choice(statuses), border=1)
        pdf.ln()
    pdf.ln(5)


def generate_pdf(output_path, total_pages=1010):
    """Generate a multi-page PDF with varied content."""
    pdf = FPDF()
    pdf.set_auto_page_break(auto=False)

    for page_num in range(1, total_pages + 1):
        pdf.add_page()

        # Footer with page number
        pdf.set_font("Helvetica", "I", 8)
        pdf.set_y(-15)
        pdf.cell(0, 10, f"Page {page_num} of {total_pages}", align="C")
        pdf.set_y(10)

        # Section heading every 10 pages
        if page_num % 10 == 1:
            section_idx = (page_num // 10) % len(SECTION_TITLES)
            pdf.set_font("Helvetica", "B", 16)
            pdf.cell(0, 12, f"Section {page_num // 10 + 1}: {SECTION_TITLES[section_idx]}", new_x="LMARGIN", new_y="NEXT")
            pdf.ln(4)
            pdf.set_font("Helvetica", "", 10)
            pdf.multi_cell(0, 5, f"This section begins on page {page_num} and covers topics related to {SECTION_TITLES[section_idx].lower()}.")
            pdf.ln(3)

        # Sub-heading
        pdf.set_font("Helvetica", "B", 12)
        pdf.cell(0, 8, f"Document Content - Page {page_num}", new_x="LMARGIN", new_y="NEXT")

        pdf.ln(2)

        # Add table every 50 pages
        if page_num % 50 == 0:
            add_table(pdf)

        # Lorem ipsum paragraphs (2-4 per page for variety)
        pdf.set_font("Helvetica", "", 10)
        num_paragraphs = random.randint(2, 4)
        for _ in range(num_paragraphs):
            paragraph = random.choice(LOREM_PARAGRAPHS)
            pdf.multi_cell(0, 5, paragraph)
            pdf.ln(3)

        # Progress logging every 100 pages
        if page_num % 100 == 0:
            print(f"  Generated {page_num}/{total_pages} pages...")

    pdf.output(output_path)
    print(f"\nPDF generation complete: {total_pages} pages")


# Generate the PDF
output_path = "/Volumes/dennis_schultz/sharepoint_testing/test_documents/large_pdfs/large_test_document_1010_pages.pdf"
print("Starting PDF generation (1,010 pages)...")
generate_pdf(output_path, total_pages=1010)

# COMMAND ----------

# DBTITLE 1,Verification Section
# MAGIC %md
# MAGIC ## Verification
# MAGIC Confirm the file was created and display its size.

# COMMAND ----------

# DBTITLE 1,Verify Output File
import os

file_path = "/Volumes/dennis_schultz/sharepoint_testing/test_documents/large_pdfs/large_test_document_1010_pages.pdf"

if os.path.exists(file_path):
    size_bytes = os.path.getsize(file_path)
    size_mb = size_bytes / (1024 * 1024)
    print(f"✓ PDF successfully created!")
    print(f"  Path: {file_path}")
    print(f"  Size: {size_mb:.2f} MB ({size_bytes:,} bytes)")
    print(f"  Pages: 1,010")
else:
    print(f"✗ ERROR: File not found at {file_path}")