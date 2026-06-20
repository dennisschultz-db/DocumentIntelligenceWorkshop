from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

PAGE_RANGE_LIMIT = 500
catalog = spark.conf.get("catalog", "main")
schema = spark.conf.get("schema", "default")
volume = spark.conf.get("volume", "personal")

# Route short versus long docs
bronze_df = spark.read.table("p01_bronze_raw_documents")
short_docs = bronze_df.filter((col("page_count") <= PAGE_RANGE_LIMIT) | col("page_count").isNull())
long_docs = bronze_df.filter(col("page_count") > PAGE_RANGE_LIMIT)

n_short = short_docs.count()
n_long = long_docs.count()
print(f"Short docs (<= {PAGE_RANGE_LIMIT} pages): {n_short}")
print(f"Long docs  (>  {PAGE_RANGE_LIMIT} pages): {n_long}")


# Persist short docs to silver
@dp.table(
    comment="SHORT parsed binary documents ingested from SharePoint"
)
def p02_silver_short_parsed():

    short_parsed = (
        short_docs
        .withColumn(
            "parsed", 
            expr(f"""
                ai_parse_document(
                    content, 
                    MAP(
                        'version', '2.0',
                        'descriptionElementTypes', 'figure',
                        'imageOutputPath', '/Volumes/{catalog}/{schema}/{volume}/slide_images/')
                )
            """)
        )
        .selectExpr(
            "*",
            "cast(1 AS BIGINT) AS chunk_count",
            "cast(parsed:error_status AS STRING) AS parse_error",
        )
    )

    return short_parsed.filter("parse_error IS NULL").drop("parse_error", "content")


# Define chunking function for long docs
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

def chunk_page_ranges(n_pages: int, chunk_size: int = PAGE_RANGE_LIMIT) -> list[str]:
    if n_pages is None or n_pages <= 0:
        return []
    return [
        f"{start}-{min(start + chunk_size - 1, n_pages)}"
        for start in range(1, n_pages + 1, chunk_size)
    ]
chunk_ranges_udf = udf(lambda n: chunk_page_ranges(n), ArrayType(StringType()))

# Parse long docs to a view
from functools import reduce

exploded = (
    long_docs
    .withColumn("page_ranges", chunk_ranges_udf("page_count"))
    .selectExpr(
        "*",
        "posexplode(page_ranges) as (chunk_idx, page_range)",
    )
)

# Bounded set of literal page-range strings, derived from the global max page count
# (no row-side collect_set). Up to ceil(max_pages / PAGE_RANGE_LIMIT) values.
max_pages_row = long_docs.selectExpr("max(page_count) AS m").first()
max_pages = (max_pages_row["m"] or 0) if max_pages_row else 0
literal_ranges = chunk_page_ranges(int(max_pages))
print(f"Literal page-range plans needed: {len(literal_ranges)} -> {literal_ranges}")

if literal_ranges:
    parsed_dfs = []
    for pr in literal_ranges:
        df = (
            exploded
            .filter(col("page_range") == pr)
            .withColumn(
                "parsed",
                expr(f"""
                    ai_parse_document(
                        content, 
                        MAP(
                            'version', '2.0',
                            'descriptionElementTypes', 'figure',
                            'pageRange', '{pr}', 
                            'imageOutputPath', '/Volumes/{catalog}/{schema}/{volume}/slide_images/')
                    )
                """)
            )
        )
        parsed_dfs.append(df)
    long_per_chunk = reduce(DataFrame.unionByName, parsed_dfs)
else:
    # No documents exceeded the limit — empty DF with a VARIANT 'parsed' column so
    # the downstream SQL view has a matching schema.
    print("No literal ranges needed.")
    long_per_chunk = exploded.withColumn("parsed", expr("parse_json(NULL)"))

long_per_chunk = long_per_chunk.selectExpr(
    "*",
    "cast(parsed:error_status AS STRING) AS parse_error",
)
long_per_chunk.createOrReplaceTempView("long_per_chunk")

# Merge chuncks of long docs together
@dp.table(
    comment="LONG parsed binary documents ingested from SharePoint"
)
def p02_silver_long_parsed():
    return spark.sql("""
        SELECT
            fileName,
            path,
            modificationTime,
            length,
            page_count,
            COUNT(*) AS chunk_count,
            parse_json(to_json(named_struct(
            'document', named_struct(
                'elements', flatten(transform(
                array_sort(
                    collect_list(struct(chunk_idx AS idx, try_cast(parsed:document:elements AS ARRAY<VARIANT>) AS elements)),
                    (l, r) -> l.idx - r.idx
                ),
                x -> x.elements
                )),
                'pages', flatten(transform(
                array_sort(
                    collect_list(struct(chunk_idx AS idx, try_cast(parsed:document:pages AS ARRAY<VARIANT>) AS pages)),
                    (l, r) -> l.idx - r.idx
                ),
                x -> x.pages
                ))
            ),
            'error_status', CAST(NULL AS STRING)
            ))) AS parsed
        FROM long_per_chunk
        WHERE parse_error IS NULL
        GROUP BY ALL;
    """)

# ============================================================
# STAGE 2: PARSE -- Extract text, images, tables from documents
# Replaces: Parsing Service + Conversion Service + Step Functions
# NOTE: pipeline_parsed_documents (MV) is now replaced by
#       pipeline_parsed_documents_v2 (ST). Drop the old table manually.
# ============================================================
@dp.table(
    comment="Parsed binary documents ingested from SharePoint"
)
def p02_silver_parsed_documents():

    return spark.table("p02_silver_short_parsed").unionByName(spark.table("p02_silver_long_parsed"))
