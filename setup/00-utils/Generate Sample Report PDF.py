# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Generate Sample Report PDF
# MAGIC This notebook generates a 5-page PDF report with a title page, headers, free text, a data table, and an embedded chart — suitable as a realistic test document for document parsing pipelines.

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install fpdf2 -q

# COMMAND ----------

# DBTITLE 1,Generate Sample Chart Image
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def create_bar_chart(output_path: str) -> None:
    """Generate a quarterly revenue bar chart and save to a file."""
    quarters = ['Q1 2026', 'Q2 2026', 'Q3 2026', 'Q4 2026']
    revenues = [42500, 57800, 63200, 71400]
    colors = ['#4472C4', '#ED7D31', '#A9D18E', '#5B9BD5']

    fig, ax = plt.subplots(figsize=(7, 3.8))
    bars = ax.bar(quarters, revenues, color=colors, edgecolor='white', width=0.55)

    ax.set_title('Annual Revenue by Quarter — FY2026', fontsize=13, fontweight='bold', pad=10)
    ax.set_ylabel('Revenue (USD $K)', fontsize=10)
    ax.set_ylim(0, max(revenues) * 1.18)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'${x:,.0f}'))
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    for bar, val in zip(bars, revenues):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 700,
            f'${val:,}',
            ha='center', va='bottom', fontsize=9, fontweight='bold'
        )

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f'Chart saved to {output_path}')

chart_path = '/tmp/report_chart.png'
create_bar_chart(chart_path)

# COMMAND ----------

# DBTITLE 1,Generate PDF Document
from fpdf import FPDF
from datetime import date

REPORT_DATE = date.today().strftime('%B %d, %Y')
TOTAL_PAGES = 5  # title page + 4 content pages


class ReportPDF(FPDF):
    """PDF subclass with a conditional footer (skipped on the title page)."""

    def footer(self) -> None:
        if self.page_no() == 1:
            return  # No footer on title page
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(120, 120, 120)
        self.cell(0, 10, f'Page {self.page_no()} of {TOTAL_PAGES}  |  {REPORT_DATE}', align='C')
        self.set_text_color(0, 0, 0)


pdf = ReportPDF()
pdf.set_auto_page_break(auto=True, margin=22)

# ── Page 1: Title Page ────────────────────────────────────────────────────────
pdf.add_page()

pdf.ln(55)
pdf.set_font('Helvetica', 'B', 28)
pdf.cell(0, 14, 'Annual Business Report', align='C', new_x='LMARGIN', new_y='NEXT')

pdf.ln(6)
pdf.set_font('Helvetica', '', 17)
pdf.cell(0, 10, 'Fiscal Year 2026', align='C', new_x='LMARGIN', new_y='NEXT')

pdf.ln(5)
pdf.set_draw_color(180, 180, 180)
pdf.set_line_width(0.5)
pdf.line(40, pdf.get_y(), 170, pdf.get_y())
pdf.ln(8)

pdf.set_font('Helvetica', 'I', 12)
pdf.cell(0, 8, 'Prepared by: Analytics Team', align='C', new_x='LMARGIN', new_y='NEXT')
pdf.ln(3)
pdf.cell(0, 8, REPORT_DATE, align='C', new_x='LMARGIN', new_y='NEXT')

pdf.ln(18)
pdf.set_font('Helvetica', '', 9)
pdf.set_text_color(140, 140, 140)
pdf.cell(0, 6, 'CONFIDENTIAL - Internal Use Only', align='C')
pdf.set_text_color(0, 0, 0)

# ── Page 2: Executive Summary ─────────────────────────────────────────────────
pdf.add_page()

pdf.set_font('Helvetica', 'B', 16)
pdf.cell(0, 10, 'Executive Summary', new_x='LMARGIN', new_y='NEXT')
pdf.set_draw_color(70, 114, 196)
pdf.set_line_width(0.6)
pdf.line(10, pdf.get_y(), 200, pdf.get_y())
pdf.ln(5)

pdf.set_font('Helvetica', '', 10)
pdf.multi_cell(0, 6,
    'This report provides a comprehensive review of business performance for fiscal year 2026. '
    'Key highlights include strong revenue growth across all four quarters, with Q4 achieving the '
    'highest quarterly revenue in company history. Operational efficiencies improved by 12% '
    'year-over-year, driven by strategic investments in automation and process optimisation.')
pdf.ln(6)

pdf.set_font('Helvetica', 'B', 13)
pdf.cell(0, 8, 'Key Highlights', new_x='LMARGIN', new_y='NEXT')
pdf.ln(2)
pdf.set_font('Helvetica', '', 10)
highlights = [
    'Total annual revenue exceeded $234.9M, a 23% increase from FY2025.',
    'Customer acquisition grew by 18%, reaching 47,200 active accounts.',
    'Operating margin improved to 31.4%, up from 28.1% in the prior year.',
    'Net Promoter Score (NPS) rose to 72, reflecting improved customer satisfaction.',
]
for h in highlights:
    pdf.cell(6, 6, chr(149))  # bullet character
    pdf.multi_cell(0, 6, f' {h}')
    pdf.ln(1)

pdf.ln(4)
pdf.set_font('Helvetica', 'B', 13)
pdf.cell(0, 8, 'Strategic Priorities', new_x='LMARGIN', new_y='NEXT')
pdf.ln(2)
pdf.set_font('Helvetica', '', 10)
pdf.multi_cell(0, 6,
    'Looking ahead to FY2027, the organisation will focus on expanding into two new geographic '
    'markets, accelerating product development cycles, and deepening partnerships with key '
    'technology vendors. Investment in data and AI capabilities will remain a top priority '
    'to sustain competitive advantage.')

# ── Page 3: Financial Summary Table ───────────────────────────────────────────
pdf.add_page()

pdf.set_font('Helvetica', 'B', 16)
pdf.cell(0, 10, 'Financial Summary', new_x='LMARGIN', new_y='NEXT')
pdf.set_draw_color(70, 114, 196)
pdf.line(10, pdf.get_y(), 200, pdf.get_y())
pdf.ln(5)

pdf.set_font('Helvetica', '', 10)
pdf.multi_cell(0, 6,
    'The table below summarises quarterly revenue, expenses, and net income for FY2026. '
    'All values are reported in USD thousands (K).')
pdf.ln(6)

# Table
col_w = [32, 42, 42, 42, 32]
headers = ['Quarter', 'Revenue ($K)', 'Expenses ($K)', 'Net Income ($K)', 'Margin (%)']
pdf.set_font('Helvetica', 'B', 10)
pdf.set_fill_color(70, 114, 196)
pdf.set_text_color(255, 255, 255)
for i, h in enumerate(headers):
    pdf.cell(col_w[i], 9, h, border=1, align='C', fill=True)
pdf.ln()
pdf.set_text_color(0, 0, 0)

rows = [
    ['Q1 2026', '$42,500', '$29,200', '$13,300', '31.3%'],
    ['Q2 2026', '$57,800', '$38,900', '$18,900', '32.7%'],
    ['Q3 2026', '$63,200', '$43,100', '$20,100', '31.8%'],
    ['Q4 2026', '$71,400', '$47,800', '$23,600', '33.1%'],
    ['Full Year', '$234,900', '$159,000', '$75,900', '32.3%'],
]
for idx, row in enumerate(rows):
    is_total = idx == len(rows) - 1
    pdf.set_font('Helvetica', 'B' if is_total else '', 10)
    fill_color = (230, 240, 255) if is_total else (255, 255, 255)
    pdf.set_fill_color(*fill_color)
    for j, cell_val in enumerate(row):
        pdf.cell(col_w[j], 8, cell_val, border=1, align='C', fill=is_total)
    pdf.ln()

pdf.ln(5)
pdf.set_font('Helvetica', 'I', 9)
pdf.set_text_color(120, 120, 120)
pdf.cell(0, 6, 'Table 1: Quarterly Financial Summary - FY2026')
pdf.set_text_color(0, 0, 0)

# ── Page 4: Chart ─────────────────────────────────────────────────────────────
pdf.add_page()

pdf.set_font('Helvetica', 'B', 16)
pdf.cell(0, 10, 'Revenue Analysis', new_x='LMARGIN', new_y='NEXT')
pdf.set_draw_color(70, 114, 196)
pdf.line(10, pdf.get_y(), 200, pdf.get_y())
pdf.ln(5)

pdf.set_font('Helvetica', '', 10)
pdf.multi_cell(0, 6,
    'The chart below illustrates the quarterly revenue trend for FY2026. Revenue growth was '
    'consistent throughout the year, with notable acceleration in H2 driven by new product '
    'launches and expanded market reach. Q4 alone accounted for 30.4% of total annual revenue.')
pdf.ln(6)

pdf.image(chart_path, x=15, w=175)
pdf.ln(4)

pdf.set_font('Helvetica', 'I', 9)
pdf.set_text_color(120, 120, 120)
pdf.cell(0, 6, 'Figure 1: Quarterly Revenue (USD $K) - FY2026', align='C')
pdf.set_text_color(0, 0, 0)

# ── Page 5: Conclusion ────────────────────────────────────────────────────────
pdf.add_page()

pdf.set_font('Helvetica', 'B', 16)
pdf.cell(0, 10, 'Conclusion & Outlook', new_x='LMARGIN', new_y='NEXT')
pdf.set_draw_color(70, 114, 196)
pdf.line(10, pdf.get_y(), 200, pdf.get_y())
pdf.ln(5)

pdf.set_font('Helvetica', '', 10)
pdf.multi_cell(0, 6,
    'Fiscal year 2026 represented a landmark year for the organisation. Sustained revenue growth, '
    'improved margins, and strong customer retention metrics validate the strategic investments '
    'made over the past two years. The company is well-positioned to capitalise on emerging '
    'market opportunities in FY2027.')
pdf.ln(6)

pdf.set_font('Helvetica', 'B', 13)
pdf.cell(0, 8, 'Recommendations', new_x='LMARGIN', new_y='NEXT')
pdf.ln(2)
pdf.set_font('Helvetica', '', 10)
recs = [
    'Increase R&D budget by 15% to accelerate new product development timelines.',
    'Expand the sales team in EMEA and APAC regions to capture international growth.',
    'Continue investment in data infrastructure to support analytics-driven decisions.',
    'Review operational processes quarterly to sustain efficiency gains from FY2026.',
]
for i, r in enumerate(recs, start=1):
    pdf.multi_cell(0, 6, f'{i}.  {r}')
    pdf.ln(2)

pdf.ln(3)
pdf.set_font('Helvetica', 'B', 13)
pdf.cell(0, 8, 'Next Steps', new_x='LMARGIN', new_y='NEXT')
pdf.ln(2)
pdf.set_font('Helvetica', '', 10)
pdf.multi_cell(0, 6,
    'The leadership team will convene in Q1 FY2027 to finalise annual targets and allocate '
    'resources in alignment with the priorities outlined in this report. Departmental scorecards '
    'will be updated to reflect the new performance benchmarks established herein.')

# ── Output ────────────────────────────────────────────────────────────────────
output_path = '../../SampleDocs/sample_report_fy2026.pdf'
pdf.output(output_path)
print(f'PDF saved to: {output_path}')
print(f'Total pages: {pdf.page}')

# COMMAND ----------

# DBTITLE 1,Verify Output
import os

file_path = '../../SampleDocs/sample_report_fy2026.pdf'

if os.path.exists(file_path):
    size_bytes = os.path.getsize(file_path)
    size_kb = size_bytes / 1024
    print(f'PDF successfully created!')
    print(f'  Path : {file_path}')
    print(f'  Size : {size_kb:.1f} KB ({size_bytes:,} bytes)')
    print(f'  Pages: 5 (1 title + 4 content)')
else:
    print(f'ERROR: File not found at {file_path}')