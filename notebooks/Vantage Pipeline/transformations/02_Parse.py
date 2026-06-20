# ============================================================
# STAGE 2: PARSE -- Extract text, images, tables from documents
# Replaces: Parsing Service + Conversion Service + Step Functions
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr, count
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from functools import reduce

PAGE_RANGE_LIMIT = 500

# ============================================================
# Define a chunking function for long docs (module level for static analysis)
def chunk_page_ranges(n_pages: int, chunk_size: int = PAGE_RANGE_LIMIT) -> list[str]:
    if n_pages is None or n_pages <= 0:
        return []
    return [
        f"{start}-{min(start + chunk_size - 1, n_pages)}"
        for start in range(1, n_pages + 1, chunk_size)
    ]
chunk_ranges_udf = udf(lambda n: chunk_page_ranges(n), ArrayType(StringType()))

# Pre-computed literal ranges covering up to 5,000 pages.
# ai_parse_document requires literal MAP values, so we generate all possible
# page-range strings upfront. Ranges with no matching documents produce empty
# DataFrames that are harmlessly unioned away.
LITERAL_RANGES = chunk_page_ranges(5000, PAGE_RANGE_LIMIT)


# ============================================================
# Process short documents
@dp.temporary_view(
    comment="SHORT parsed binary documents ingested from SharePoint"
)
def p02_silver_short_parsed():
    catalog = spark.conf.get("catalog", "main")
    schema  = spark.conf.get("schema", "default")
    volume  = spark.conf.get("volume", "personal")

    bronze_df = spark.read.table("p01_bronze_raw_documents")
    short_docs = bronze_df.filter((col("page_count") <= PAGE_RANGE_LIMIT) | col("page_count").isNull())

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


# ============================================================
# Process long documents by chunking then merging
@dp.temporary_view(
    comment="LONG parsed binary documents ingested from SharePoint"
)
def p02_silver_long_parsed():
    catalog = spark.conf.get("catalog", "main")
    schema  = spark.conf.get("schema", "default")
    volume  = spark.conf.get("volume", "personal")

    bronze_df = spark.read.table("p01_bronze_raw_documents")
    long_docs = bronze_df.filter(col("page_count") > PAGE_RANGE_LIMIT)

    exploded = (
        long_docs
        .withColumn("page_ranges", chunk_ranges_udf("page_count"))
        .selectExpr(
            "*",
            "posexplode(page_ranges) as (chunk_idx, page_range)",
        )
    )

    # Parse each chunk with a literal pageRange value (ai_parse_document requires literals).
    # LITERAL_RANGES covers up to 5,000 pages; ranges with no matching rows are empty.
    parsed_dfs = [
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
        for pr in LITERAL_RANGES
    ]
    long_per_chunk = reduce(DataFrame.unionByName, parsed_dfs)
    long_per_chunk = long_per_chunk.selectExpr(
        "*",
        "cast(parsed:error_status AS STRING) AS parse_error",
    )

    # Merge chunks back per document
    return (
        long_per_chunk
        .filter("parse_error IS NULL")
        .groupBy("fileName", "path", "modificationTime", "length", "page_count")
        .agg(
            count("*").alias("chunk_count"),
            expr("""parse_json(to_json(named_struct(
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
            ))) AS parsed""")
        )
    )

# ============================================================
# Union short and long parsed documents
@dp.table(
    comment="Parsed binary documents ingested from SharePoint"
)
def p02_silver_parsed_documents():

    return (
        spark.read.table("p02_silver_short_parsed")
                .unionByName(spark.read.table("p02_silver_long_parsed"))
    )
