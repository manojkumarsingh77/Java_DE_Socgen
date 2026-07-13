// ─── Java Source Files for Customer 360 Join Model ───────────────────────────

export const files: Record<string, { label: string; lang: string; code: string }> = {

  // ─── 1. Main Entry Point ─────────────────────────────────────────────────
  "Customer360App.java": {
    label: "Customer360App.java",
    lang: "java",
    code: `package com.customer360.spark;

import com.customer360.spark.config.SparkSessionConfig;
import com.customer360.spark.pipeline.Customer360Pipeline;
import com.customer360.spark.util.MetricsLogger;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║   Customer 360 — 6-Way Join Model (30-min SLA)                          ║
 * ║   Demonstrates: AQE Internals · Partition Strategy · Bucketing ·        ║
 * ║                 Join Strategy Selection                                  ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 *
 * Architecture Overview
 * ─────────────────────
 *  Domain Tables (6):
 *    1. customers        ~500 M rows  — core identity
 *    2. orders           ~2  B rows   — transactional backbone
 *    3. products         ~10 M rows   — catalog (broadcast candidate)
 *    4. web_events       ~5  B rows   — clickstream
 *    5. support_tickets  ~200 M rows  — CRM signals
 *    6. loyalty_rewards  ~300 M rows  — engagement programme
 *
 *  Join Strategy per hop:
 *    customers ▶ orders          → Sort-Merge Join (both large)
 *    + products                  → Broadcast Hash Join (10 M rows ≈ 480 MB)
 *    + web_events                → Sort-Merge Join + AQE skew split
 *    + support_tickets           → Shuffle Hash Join (medium, uniform)
 *    + loyalty_rewards           → Sort-Merge Join (bucketed, no shuffle)
 *
 *  SLA Budget (30 min on a 20-executor × 16-core × 64 GB cluster):
 *    Data generation   ~2  min
 *    Bucketing setup   ~3  min
 *    6-way join DAG    ~18 min
 *    Aggregation       ~4  min
 *    Write / report    ~3  min
 */
public class Customer360App {

    private static final Logger LOG = LoggerFactory.getLogger(Customer360App.class);

    public static void main(String[] args) throws Exception {
        long wallStart = System.currentTimeMillis();
        LOG.info("▶  Customer 360 pipeline starting …");

        SparkSession spark = SparkSessionConfig.create("Customer360-6WayJoin");
        MetricsLogger metrics = new MetricsLogger(spark);

        try {
            Customer360Pipeline pipeline = new Customer360Pipeline(spark, metrics);
            pipeline.run();
        } finally {
            long elapsed = (System.currentTimeMillis() - wallStart) / 1_000;
            LOG.info("✔  Pipeline finished in {} s ({} min)", elapsed, elapsed / 60);
            metrics.printSummary();
            spark.stop();
        }
    }
}
`,
  },

  // ─── 2. SparkSession Configuration ────────────────────────────────────────
  "SparkSessionConfig.java": {
    label: "SparkSessionConfig.java",
    lang: "java",
    code: `package com.customer360.spark.config;

import org.apache.spark.sql.SparkSession;

/**
 * Centralised Spark configuration for the Customer 360 pipeline.
 *
 * Key AQE settings explained
 * ──────────────────────────
 * spark.sql.adaptive.enabled
 *   Master switch — must be true for all AQE features below.
 *
 * spark.sql.adaptive.coalescePartitions.enabled
 *   After each shuffle Spark inspects actual partition byte-sizes and
 *   merges adjacent small partitions into target advisoryPartitionSizeInBytes.
 *   Prevents the "10 000 × 1 MB task" anti-pattern after a selective filter.
 *
 * spark.sql.adaptive.advisoryPartitionSizeInBytes (128 MB)
 *   AQE coalesces until each partition is ≈ this size. Tuned for 64 GB
 *   executors: 128 MB * 4 threads = 512 MB working set per core.
 *
 * spark.sql.adaptive.skewJoin.enabled
 *   Detects hot partitions (> skewedPartitionThresholdInBytes AND
 *   > skewedPartitionFactor × median) and auto-splits them.
 *
 * spark.sql.adaptive.skewJoin.skewedPartitionFactor (5)
 *   A partition is "skewed" if it is 5× the median partition size.
 *
 * spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes (256 MB)
 *   Minimum absolute size before a partition is split regardless of factor.
 *
 * spark.sql.adaptive.autoBroadcastJoinThreshold (200 MB)
 *   At runtime, if the smaller side of a join materialises below this value,
 *   AQE converts Sort-Merge → Broadcast-Hash without restarting the query.
 *
 * spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold (64 MB)
 *   When all post-shuffle partitions are < this threshold AQE prefers
 *   Shuffled-Hash over Sort-Merge (no sort phase, lower latency).
 */
public final class SparkSessionConfig {

    private SparkSessionConfig() {}

    public static SparkSession create(String appName) {
        return SparkSession.builder()
            .appName(appName)
            .master("local[*]")   // change to "yarn" or "k8s://…" in production

            // ── Core AQE ──────────────────────────────────────────────────
            .config("spark.sql.adaptive.enabled",                              "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled",           "true")
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes",         "128m")
            .config("spark.sql.adaptive.coalescePartitions.minPartitionNum",   "50")

            // ── Skew Join Handling ─────────────────────────────────────────
            .config("spark.sql.adaptive.skewJoin.enabled",                     "true")
            .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor",       "5")
            .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")

            // ── Dynamic Join Strategy ─────────────────────────────────────
            .config("spark.sql.adaptive.autoBroadcastJoinThreshold",           "209715200") // 200 MB
            .config("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", "67108864")  //  64 MB

            // ── Static Broadcast Threshold (pre-AQE static planner) ───────
            .config("spark.sql.autoBroadcastJoinThreshold",                    "104857600") // 100 MB

            // ── Shuffle & I/O ─────────────────────────────────────────────
            .config("spark.sql.shuffle.partitions",                            "800")
            .config("spark.sql.files.maxPartitionBytes",                       "134217728") // 128 MB
            .config("spark.sql.files.openCostInBytes",                         "8388608")   //   8 MB

            // ── Bucketing ─────────────────────────────────────────────────
            .config("spark.sql.sources.bucketing.enabled",                     "true")
            .config("spark.sql.sources.bucketing.autoBucketedScanEnabled",     "true")

            // ── Columnar / Vectorised Read ─────────────────────────────────
            .config("spark.sql.parquet.enableVectorizedReader",                "true")
            .config("spark.sql.inMemoryColumnarStorage.compressed",            "true")
            .config("spark.sql.columnVector.offheap.enabled",                  "true")

            // ── Memory ────────────────────────────────────────────────────
            .config("spark.executor.memory",                                   "48g")
            .config("spark.driver.memory",                                     "8g")
            .config("spark.memory.fraction",                                   "0.8")
            .config("spark.memory.storageFraction",                            "0.3")

            // ── Serialisation ──────────────────────────────────────────────
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.registrationRequired", "false")

            // ── Speculation & Locality ────────────────────────────────────
            .config("spark.speculation",                  "true")
            .config("spark.speculation.multiplier",       "3")
            .config("spark.locality.wait",                "3s")

            .enableHiveSupport()
            .getOrCreate();
    }
}
`,
  },

  // ─── 3. Data Generator ────────────────────────────────────────────────────
  "DataGenerator.java": {
    label: "DataGenerator.java",
    lang: "java",
    code: `package com.customer360.spark.generator;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

import java.time.LocalDate;

/**
 * Synthetic data generator for all 6 Customer 360 domain tables.
 *
 * Scale parameters (adjust for local demo vs. production):
 *   LOCAL_SCALE  = 0.001  →  ~500 K customers, fast local run
 *   PROD_SCALE   = 1.0    →  ~500 M customers, full cluster run
 *
 * Intentional skew in orders.customer_id (top 0.01% customers have 200×
 * average order volume) to demonstrate AQE skew-join splitting.
 */
public class DataGenerator {

    // ── Scale factor ─ set to 0.001 for local laptop demo ─────────────────
    private static final double SCALE = Double.parseDouble(
        System.getProperty("demo.scale", "0.001"));

    private static final long CUSTOMERS       = (long)(500_000_000 * SCALE);
    private static final long ORDERS          = (long)(2_000_000_000 * SCALE);
    private static final long PRODUCTS        = (long)(10_000_000  * SCALE);
    private static final long WEB_EVENTS      = (long)(5_000_000_000L * SCALE);
    private static final long SUPPORT_TICKETS = (long)(200_000_000 * SCALE);
    private static final long LOYALTY_REWARDS = (long)(300_000_000 * SCALE);

    private final SparkSession spark;

    public DataGenerator(SparkSession spark) { this.spark = spark; }

    // ── 1. Customers ──────────────────────────────────────────────────────
    public Dataset<Row> generateCustomers() {
        return spark.range(1, CUSTOMERS + 1).toDF("customer_id")
            .withColumn("first_name",     expr("concat('FirstName_', customer_id % 50000)"))
            .withColumn("last_name",      expr("concat('LastName_',  customer_id % 80000)"))
            .withColumn("email",          expr("concat('user', customer_id, '@example.com')"))
            .withColumn("country_code",   expr("element_at(array('US','GB','DE','FR','IN','BR','AU','CA'), cast(rand()*8+1 as int))"))
            .withColumn("age_bucket",     expr("cast(rand()*6 as int) * 10 + 20")) // 20-70
            .withColumn("segment",        expr("element_at(array('GOLD','SILVER','BRONZE','PLATINUM'), cast(rand()*4+1 as int))"))
            .withColumn("created_date",   expr("date_sub(current_date(), cast(rand()*1825 as int))"))
            .withColumn("is_active",      expr("rand() > 0.05"))
            .withColumn("lifetime_value", expr("round(rand() * 50000, 2)"))
            .withColumn("etl_ts",         current_timestamp());
    }

    // ── 2. Orders (with intentional skew on top VIP customers) ───────────
    public Dataset<Row> generateOrders() {
        // 99.99 % of orders use uniform customer_id distribution
        Dataset<Row> normal = spark.range(1, (long)(ORDERS * 0.9999) + 1).toDF("order_id")
            .withColumn("customer_id",  expr("cast(rand() * " + CUSTOMERS + " + 1 as long)"))
            .withColumn("product_id",   expr("cast(rand() * " + PRODUCTS  + " + 1 as long)"))
            .withColumn("order_status", expr("element_at(array('PLACED','SHIPPED','DELIVERED','RETURNED','CANCELLED'), cast(rand()*5+1 as int))"))
            .withColumn("order_date",   expr("date_sub(current_date(), cast(rand()*730 as int))"))
            .withColumn("amount",       expr("round(rand() * 5000 + 1, 2)"))
            .withColumn("quantity",     expr("cast(rand() * 20 + 1 as int)"))
            .withColumn("channel",      expr("element_at(array('WEB','APP','STORE','PHONE'), cast(rand()*4+1 as int))"))
            .withColumn("etl_ts",       current_timestamp());

        // 0.01 % are VIP customers — generate heavy skew (200× volume)
        Dataset<Row> skewed = spark.range(1, (long)(ORDERS * 0.0001) + 1).toDF("order_id")
            .withColumn("customer_id",  expr("cast(rand() * 1000 + 1 as long)"))  // top-1000 VIPs
            .withColumn("product_id",   expr("cast(rand() * " + PRODUCTS  + " + 1 as long)"))
            .withColumn("order_status", lit("DELIVERED"))
            .withColumn("order_date",   expr("date_sub(current_date(), cast(rand()*730 as int))"))
            .withColumn("amount",       expr("round(rand() * 25000 + 1000, 2)"))
            .withColumn("quantity",     expr("cast(rand() * 100 + 50 as int)"))
            .withColumn("channel",      lit("VIP"))
            .withColumn("etl_ts",       current_timestamp());

        return normal.union(skewed);
    }

    // ── 3. Products (small — broadcast candidate) ─────────────────────────
    public Dataset<Row> generateProducts() {
        return spark.range(1, PRODUCTS + 1).toDF("product_id")
            .withColumn("product_name",  expr("concat('Product_', product_id)"))
            .withColumn("category",      expr("element_at(array('ELECTRONICS','APPAREL','FOOD','SPORTS','HOME','BOOKS'), cast(rand()*6+1 as int))"))
            .withColumn("sub_category",  expr("concat('SubCat_', cast(rand()*20+1 as int))"))
            .withColumn("brand",         expr("concat('Brand_', cast(rand()*500+1 as int))"))
            .withColumn("unit_price",    expr("round(rand() * 2000 + 1, 2)"))
            .withColumn("cost_price",    expr("round(rand() * 1200 + 0.5, 2)"))
            .withColumn("margin_pct",    expr("round(rand() * 60 + 10, 2)"))
            .withColumn("is_active",     expr("rand() > 0.02"))
            .withColumn("etl_ts",        current_timestamp());
    }

    // ── 4. Web Events ──────────────────────────────────────────────────────
    public Dataset<Row> generateWebEvents() {
        return spark.range(1, WEB_EVENTS + 1).toDF("event_id")
            .withColumn("customer_id",   expr("cast(rand() * " + CUSTOMERS + " + 1 as long)"))
            .withColumn("session_id",    expr("concat('sess_', cast(rand()*500000000 as long))"))
            .withColumn("event_type",    expr("element_at(array('PAGE_VIEW','CLICK','ADD_TO_CART','CHECKOUT','PURCHASE','WISHLIST'), cast(rand()*6+1 as int))"))
            .withColumn("page_url",      expr("concat('/page/', cast(rand()*10000 as int))"))
            .withColumn("device_type",   expr("element_at(array('MOBILE','DESKTOP','TABLET'), cast(rand()*3+1 as int))"))
            .withColumn("referrer",      expr("element_at(array('ORGANIC','PAID','EMAIL','SOCIAL','DIRECT'), cast(rand()*5+1 as int))"))
            .withColumn("duration_secs", expr("cast(rand()*1800 as int)"))
            .withColumn("event_ts",      expr("to_timestamp(date_sub(current_date(), cast(rand()*90 as int)))"))
            .withColumn("etl_ts",        current_timestamp());
    }

    // ── 5. Support Tickets ─────────────────────────────────────────────────
    public Dataset<Row> generateSupportTickets() {
        return spark.range(1, SUPPORT_TICKETS + 1).toDF("ticket_id")
            .withColumn("customer_id",   expr("cast(rand() * " + CUSTOMERS + " + 1 as long)"))
            .withColumn("category",      expr("element_at(array('BILLING','SHIPPING','PRODUCT','ACCOUNT','RETURNS'), cast(rand()*5+1 as int))"))
            .withColumn("priority",      expr("element_at(array('LOW','MEDIUM','HIGH','CRITICAL'), cast(rand()*4+1 as int))"))
            .withColumn("status",        expr("element_at(array('OPEN','IN_PROGRESS','RESOLVED','CLOSED'), cast(rand()*4+1 as int))"))
            .withColumn("satisfaction",  expr("cast(rand()*5+1 as int)"))
            .withColumn("resolution_hours", expr("cast(rand()*240 as int)"))
            .withColumn("created_ts",    expr("to_timestamp(date_sub(current_date(), cast(rand()*365 as int)))"))
            .withColumn("etl_ts",        current_timestamp());
    }

    // ── 6. Loyalty Rewards ─────────────────────────────────────────────────
    public Dataset<Row> generateLoyaltyRewards() {
        return spark.range(1, LOYALTY_REWARDS + 1).toDF("reward_id")
            .withColumn("customer_id",   expr("cast(rand() * " + CUSTOMERS + " + 1 as long)"))
            .withColumn("points_earned", expr("cast(rand()*50000 as int)"))
            .withColumn("points_redeemed",expr("cast(rand()*30000 as int)"))
            .withColumn("tier",          expr("element_at(array('CLASSIC','SILVER','GOLD','DIAMOND'), cast(rand()*4+1 as int))"))
            .withColumn("program_code",  expr("element_at(array('CORE','PARTNER','VIP','REFERRAL'), cast(rand()*4+1 as int))"))
            .withColumn("expiry_date",   expr("date_add(current_date(), cast(rand()*365 as int))"))
            .withColumn("etl_ts",        current_timestamp());
    }
}
`,
  },

  // ─── 4. Partition Strategy ────────────────────────────────────────────────
  "PartitionStrategy.java": {
    label: "PartitionStrategy.java",
    lang: "java",
    code: `package com.customer360.spark.partition;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

/**
 * ╔═══════════════════════════════════════════════════════════════════╗
 * ║  Partition Strategy Modelling — Key Concepts                     ║
 * ╠═══════════════════════════════════════════════════════════════════╣
 * ║  PARTITIONING  → organises files on disk by column value         ║
 * ║                  (directory-per-value, good for FILTER pruning)  ║
 * ║  BUCKETING     → pre-hashes rows into N fixed buckets on a key   ║
 * ║                  (bucket files co-located, good for JOIN / AGG)  ║
 * ╚═══════════════════════════════════════════════════════════════════╝
 *
 * When to PARTITION (by column) vs BUCKET (by join key):
 * ──────────────────────────────────────────────────────
 *  Use partitioning when:
 *    • Queries almost always filter on that column (e.g. order_date, country_code)
 *    • Cardinality is LOW (< 10 000 unique values) — avoids file explosion
 *    • Downstream consumers (Athena, Hive, Presto) rely on partition pruning
 *
 *  Use bucketing when:
 *    • You repeatedly join two LARGE tables on the SAME key (e.g. customer_id)
 *    • You want to ELIMINATE the shuffle in Sort-Merge Join
 *    • Cardinality is HIGH (millions of distinct join key values)
 *    • Both tables must be bucketed on the same key with the SAME bucket count
 *      (or one being a multiple: 200 buckets ÷ 50 buckets = 4× — OK)
 *
 *  Bucket count formula (rule of thumb):
 *    buckets = max(nextPowerOf2(totalDataGB / targetFileSizeGB), minBuckets)
 *    e.g.  customers 500 GB / 0.5 GB = 1024 buckets
 *          orders   2000 GB / 0.5 GB = 4096 buckets (use 4× customers → ✓)
 *
 *  Caveats:
 *    • Bucketing is a WRITE-TIME decision — re-bucketing requires full rewrite
 *    • Mismatched bucket counts force an extra Exchange (negates the benefit)
 *    • AQE can auto-broadcast the smaller side even if not bucketed
 */
public class PartitionStrategy {

    // ─── Bucket counts (powers-of-2 so Spark can align them) ─────────────
    public static final int CUSTOMERS_BUCKETS       = 1024;
    public static final int ORDERS_BUCKETS          = 4096;  // 4× customers
    public static final int WEB_EVENTS_BUCKETS      = 4096;  // 4× customers
    public static final int SUPPORT_TICKETS_BUCKETS = 1024;  // 1× customers
    public static final int LOYALTY_REWARDS_BUCKETS = 1024;  // 1× customers
    // products → broadcast-only (no bucketing needed)

    // ─── Persist each table with the agreed strategy ───────────────────────
    public static void persistCustomers(Dataset<Row> df) {
        df.write()
            .mode(SaveMode.Overwrite)
            // Partition by segment → fast segment-level scans (low cardinality: 4 values)
            .partitionBy("segment")
            // Bucket by customer_id → join with orders/tickets/rewards without shuffle
            .bucketBy(CUSTOMERS_BUCKETS, "customer_id")
            .sortBy("customer_id")
            .saveAsTable("c360.customers_bucketed");
    }

    public static void persistOrders(Dataset<Row> df) {
        df.write()
            .mode(SaveMode.Overwrite)
            // Partition by order_date (year-month) to support time-range queries
            .partitionBy("channel")
            // 4× bucket count means 4 order buckets pair with every 1 customer bucket
            .bucketBy(ORDERS_BUCKETS, "customer_id")
            .sortBy("customer_id", "order_date")
            .saveAsTable("c360.orders_bucketed");
    }

    public static void persistWebEvents(Dataset<Row> df) {
        df.write()
            .mode(SaveMode.Overwrite)
            .partitionBy("device_type")
            .bucketBy(WEB_EVENTS_BUCKETS, "customer_id")
            .sortBy("customer_id", "event_ts")
            .saveAsTable("c360.web_events_bucketed");
    }

    public static void persistSupportTickets(Dataset<Row> df) {
        df.write()
            .mode(SaveMode.Overwrite)
            .partitionBy("priority")
            .bucketBy(SUPPORT_TICKETS_BUCKETS, "customer_id")
            .sortBy("customer_id")
            .saveAsTable("c360.support_tickets_bucketed");
    }

    public static void persistLoyaltyRewards(Dataset<Row> df) {
        df.write()
            .mode(SaveMode.Overwrite)
            .partitionBy("tier")
            .bucketBy(LOYALTY_REWARDS_BUCKETS, "customer_id")
            .sortBy("customer_id")
            .saveAsTable("c360.loyalty_rewards_bucketed");
    }

    /**
     * Repartition helper — call before an expensive join to align
     * in-memory partitions with the bucketed on-disk layout.
     * Only used when reading NON-bucketed DataFrames into the pipeline.
     */
    public static Dataset<Row> alignPartitions(Dataset<Row> df,
                                               String joinKey,
                                               int targetPartitions) {
        return df.repartition(targetPartitions, col(joinKey));
    }
}
`,
  },

  // ─── 5. Join Strategy Selector ────────────────────────────────────────────
  "JoinStrategySelector.java": {
    label: "JoinStrategySelector.java",
    lang: "java",
    code: `package com.customer360.spark.join;

import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;

/**
 * ╔══════════════════════════════════════════════════════════════════════╗
 * ║  Spark Join Strategies — Decision Matrix                            ║
 * ╠══════════════════════════════════════════════════════════════════════╣
 * ║  Strategy               Trigger / Condition                         ║
 * ╠══════════════════════════════════════════════════════════════════════╣
 * ║  Broadcast Hash Join    smaller side < autoBroadcastJoinThreshold   ║
 * ║  (BHJ)                  No shuffle on either side.                  ║
 * ║                         Best for: fact × dimension                  ║
 * ╠══════════════════════════════════════════════════════════════════════╣
 * ║  Shuffle Hash Join      Both sides shuffled by join key.            ║
 * ║  (SHJ)                  Build hash map from smaller side.           ║
 * ║                         Better than SMJ when data already uniform.  ║
 * ╠══════════════════════════════════════════════════════════════════════╣
 * ║  Sort-Merge Join        Both sides sorted after shuffle.            ║
 * ║  (SMJ)                  Handles arbitrarily large tables.           ║
 * ║                         Bucketing eliminates the shuffle stage.     ║
 * ╠══════════════════════════════════════════════════════════════════════╣
 * ║  Broadcast Nested Loop  No join key — cross join fallback.          ║
 * ║  (BNLJ)                 Avoid in production (O(m×n) complexity).    ║
 * ╠══════════════════════════════════════════════════════════════════════╣
 * ║  Cartesian Product      Explicit cross join. Explicit hint only.    ║
 * ╚══════════════════════════════════════════════════════════════════════╝
 *
 * AQE Runtime Conversions (all automatic when AQE is enabled):
 *   SMJ  → BHJ   if actual size of smaller side < autoBroadcastJoinThreshold
 *   SMJ  → SHJ   if all partitions < maxShuffledHashJoinLocalMapThreshold
 *   SMJ (skewed) → split-and-join replicas for hot partitions
 */
public class JoinStrategySelector {

    private static final String JOIN_KEY = "customer_id";

    /**
     * HOP 1: customers × orders
     * Strategy: Sort-Merge Join (both tables are large; bucketed = no shuffle)
     * AQE role: monitors for skewed customer_id buckets and splits them
     */
    public static Dataset<Row> joinCustomersOrders(Dataset<Row> customers,
                                                   Dataset<Row> orders) {
        return customers
            // MERGE hint is advisory; Spark uses SMJ unless AQE overrides at runtime
            .hint("MERGE")
            .join(orders.hint("MERGE"), JOIN_KEY, "left")
            .select(
                customers.col("customer_id"),
                customers.col("first_name"),
                customers.col("last_name"),
                customers.col("email"),
                customers.col("country_code"),
                customers.col("age_bucket"),
                customers.col("segment"),
                customers.col("lifetime_value"),
                customers.col("is_active"),
                orders.col("order_id"),
                orders.col("product_id"),
                orders.col("order_date"),
                orders.col("order_status"),
                orders.col("amount").alias("order_amount"),
                orders.col("quantity"),
                orders.col("channel")
            );
    }

    /**
     * HOP 2: + products
     * Strategy: Broadcast Hash Join — products ≈ 480 MB fits in memory.
     * AQE role: validates size post-shuffle; converts SMJ → BHJ if estimated
     *           size was wrong at plan time.
     * BROADCAST hint forces BHJ even if static planner disagrees.
     */
    public static Dataset<Row> joinProducts(Dataset<Row> base,
                                            Dataset<Row> products) {
        return base.join(
            broadcast(products),          // explicit broadcast hint
            base.col("product_id").equalTo(products.col("product_id")),
            "left"
        ).drop(products.col("product_id"))
         .withColumnRenamed("product_name", "prd_name")
         .withColumnRenamed("category",     "prd_category")
         .withColumnRenamed("unit_price",   "prd_unit_price")
         .withColumnRenamed("margin_pct",   "prd_margin_pct")
         .drop("cost_price", "sub_category", "brand", "is_active");
    }

    /**
     * HOP 3: + web_events
     * Strategy: Sort-Merge Join with AQE skew detection.
     * web_events is 5 B rows — too large for broadcast or hash join.
     * We aggregate first to reduce to one row per customer per device.
     * AQE handles residual skew on top-1000 VIP customer_ids.
     */
    public static Dataset<Row> joinWebEvents(Dataset<Row> base,
                                             Dataset<Row> webEvents) {
        Dataset<Row> webAgg = webEvents
            .groupBy("customer_id", "device_type")
            .agg(
                count("event_id").alias("total_events"),
                countDistinct("session_id").alias("total_sessions"),
                sum(when(col("event_type").equalTo("PURCHASE"), 1).otherwise(0))
                    .alias("web_purchases"),
                avg("duration_secs").alias("avg_session_secs")
            );

        return base.hint("MERGE").join(
            webAgg.hint("MERGE"),
            JOIN_KEY, "left"
        );
    }

    /**
     * HOP 4: + support_tickets
     * Strategy: Shuffle Hash Join — support_tickets is medium-sized (~8 GB).
     * After AQE coalesces partitions, each partition < 64 MB threshold,
     * triggering automatic SMJ → SHJ conversion.
     * SHUFFLE_HASH hint pre-declares intent; AQE confirms at runtime.
     */
    public static Dataset<Row> joinSupportTickets(Dataset<Row> base,
                                                  Dataset<Row> tickets) {
        Dataset<Row> ticketsAgg = tickets
            .groupBy("customer_id")
            .agg(
                count("ticket_id").alias("total_tickets"),
                avg("satisfaction").alias("avg_satisfaction"),
                avg("resolution_hours").alias("avg_resolution_hrs"),
                sum(when(col("priority").equalTo("CRITICAL"), 1).otherwise(0))
                    .alias("critical_tickets")
            );

        return base.hint("SHUFFLE_HASH").join(
            ticketsAgg.hint("SHUFFLE_HASH"),
            JOIN_KEY, "left"
        );
    }

    /**
     * HOP 5: + loyalty_rewards
     * Strategy: Sort-Merge Join via bucketed tables — NO shuffle generated.
     * Both tables bucketed on customer_id with matching bucket counts (1024).
     * Spark reads bucket files co-located → pure merge with zero network I/O.
     */
    public static Dataset<Row> joinLoyaltyRewards(Dataset<Row> base,
                                                  Dataset<Row> loyalty) {
        Dataset<Row> loyaltyAgg = loyalty
            .groupBy("customer_id")
            .agg(
                sum("points_earned").alias("total_points_earned"),
                sum("points_redeemed").alias("total_points_redeemed"),
                (sum("points_earned").minus(sum("points_redeemed"))).alias("points_balance"),
                max("tier").alias("loyalty_tier"),
                count("reward_id").alias("reward_events")
            );

        // No hint needed — Spark detects matching bucket layout and skips Exchange
        return base.join(loyaltyAgg, JOIN_KEY, "left");
    }
}
`,
  },

  // ─── 6. AQE Monitor ───────────────────────────────────────────────────────
  "AQEMonitor.java": {
    label: "AQEMonitor.java",
    lang: "java",
    code: `package com.customer360.spark.aqe;

import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.*;
import org.apache.spark.scheduler.*;
import org.apache.spark.SparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runtime AQE internals monitor.
 *
 * What this tracks
 * ────────────────
 *  • Join strategy changes (SMJ → BHJ, SMJ → SHJ)
 *  • Skew partition splits per join stage
 *  • Partition coalesce events (N initial → M final partitions)
 *  • Stage-level shuffle bytes read/written
 *  • Task speculation events
 *
 * How AQE works internally (re-optimise loop):
 * ─────────────────────────────────────────────
 *  1. AdaptiveSparkPlanExec wraps every query with AQE enabled.
 *  2. Spark executes leaf stages first (table scans → shuffle write).
 *  3. After each stage completes, MapOutputStatistics (file sizes per
 *     partition) are collected from the ShuffleManager via
 *     SparkContext.dagScheduler.getShuffleDependencies().
 *  4. OptimizeSkewedJoin rule splits partitions > skewedPartitionFactor
 *     × medianPartitionSize into sub-partitions.
 *  5. CoalesceShufflePartitions rule merges adjacent small partitions
 *     until each is ≥ advisoryPartitionSizeInBytes.
 *  6. DynamicJoinSelection rule re-evaluates join strategies using
 *     actual (not estimated) row counts and byte sizes.
 *  7. A new QueryStage is created for the reoptimised sub-plan and
 *     the execute-reoptimise-execute loop continues.
 *
 * Observability hooks used here:
 *  • SparkListener.onTaskEnd  → per-task shuffle metrics
 *  • SparkListener.onStageCompleted → stage-level aggregates
 *  • QueryExecution.toString  → parsed/analysed/optimised/physical plans
 */
public class AQEMonitor implements SparkListener {

    private static final Logger LOG = LoggerFactory.getLogger(AQEMonitor.class);

    private final SparkSession spark;
    private final Map<Integer, StageMetrics> stageMetrics = new ConcurrentHashMap<>();
    private final AtomicLong totalSkewSplits   = new AtomicLong(0);
    private final AtomicLong totalCoalesces    = new AtomicLong(0);
    private final AtomicLong joinConversions   = new AtomicLong(0);

    public AQEMonitor(SparkSession spark) {
        this.spark = spark;
        spark.sparkContext().addSparkListener(this);
    }

    // ── SparkListener callbacks ───────────────────────────────────────────

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        var info = stageCompleted.stageInfo();
        var taskMetrics = info.taskMetrics();

        StageMetrics m = new StageMetrics();
        m.stageId         = info.stageId();
        m.stageName       = info.name();
        m.numTasks        = info.numTasks();
        m.shuffleWriteBytes = taskMetrics.shuffleWriteMetrics().bytesWritten();
        m.shuffleReadBytes  = taskMetrics.shuffleReadMetrics().totalBytesRead();
        m.durationMs      = info.completionTime().getOrElse(() -> 0L)
                          - info.submissionTime().getOrElse(() -> 0L);

        stageMetrics.put(m.stageId, m);

        LOG.info("[AQE] Stage {} ('{}') — tasks={}, shuffleWrite={} MB, shuffleRead={} MB, duration={} s",
            m.stageId, m.stageName, m.numTasks,
            m.shuffleWriteBytes / 1_048_576,
            m.shuffleReadBytes  / 1_048_576,
            m.durationMs / 1_000);
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        // Flag speculated tasks (evidence of skew)
        if (taskEnd.taskInfo().speculative()) {
            LOG.warn("[AQE] Speculative task detected — stage {} task {} — possible skew partition",
                taskEnd.stageId(), taskEnd.taskInfo().taskId());
        }
    }

    // ── Query plan introspection ──────────────────────────────────────────

    /**
     * Prints the physical plan of a DataFrame and highlights AQE decisions.
     * Look for these markers in the output:
     *   AdaptiveSparkPlan isFinalPlan=false   → plan may still change
     *   AdaptiveSparkPlan isFinalPlan=true    → AQE finished re-optimising
     *   BroadcastHashJoin                     → AQE converted SMJ → BHJ
     *   ShuffledHashJoin                      → AQE converted SMJ → SHJ
     *   SortMergeJoin (isSkewJoin=true)       → AQE is splitting hot keys
     */
    public void explainPlan(Dataset<Row> df, String label) {
        LOG.info("════ Physical Plan [{}] ════", label);
        df.explain("formatted");   // "formatted" mode shows AQE annotations
    }

    /**
     * Capture AQE decisions by comparing initial vs final physical plan.
     * Uses SparkSession.sql to force plan materialisation.
     */
    public void captureAQEDecisions(Dataset<Row> df, String queryLabel) {
        df.createOrReplaceTempView("_aqe_probe_" + queryLabel.replace(" ", "_"));
        var qe = spark.sql("SELECT COUNT(*) FROM _aqe_probe_" + queryLabel.replace(" ", "_"))
                      .queryExecution();

        String physPlan = qe.executedPlan().toString();
        if (physPlan.contains("BroadcastHashJoin")) {
            joinConversions.incrementAndGet();
            LOG.info("[AQE] ✔ Join conversion → BroadcastHashJoin detected for '{}'", queryLabel);
        }
        if (physPlan.contains("isSkewJoin=true")) {
            long splits = physPlan.lines()
                .filter(l -> l.contains("isSkewJoin=true")).count();
            totalSkewSplits.addAndGet(splits);
            LOG.info("[AQE] ✔ Skew splits detected ({}) for '{}'", splits, queryLabel);
        }
    }

    // ── Summary report ────────────────────────────────────────────────────

    public void printAQESummary() {
        LOG.info("══════════════════════════════════════════════");
        LOG.info("  AQE Runtime Decision Summary");
        LOG.info("  Join strategy conversions : {}", joinConversions.get());
        LOG.info("  Skew partition splits     : {}", totalSkewSplits.get());
        LOG.info("  Partition coalesces       : {}", totalCoalesces.get());
        LOG.info("  Stages completed          : {}", stageMetrics.size());
        long totalShuffleGB = stageMetrics.values().stream()
            .mapToLong(m -> m.shuffleWriteBytes).sum() / 1_073_741_824L;
        LOG.info("  Total shuffle write       : {} GB", totalShuffleGB);
        LOG.info("══════════════════════════════════════════════");
    }

    // ── Inner DTO ─────────────────────────────────────────────────────────

    static class StageMetrics {
        int    stageId;
        String stageName;
        int    numTasks;
        long   shuffleWriteBytes;
        long   shuffleReadBytes;
        long   durationMs;
    }
}
`,
  },

  // ─── 7. Customer360Pipeline (Orchestrator) ────────────────────────────────
  "Customer360Pipeline.java": {
    label: "Customer360Pipeline.java",
    lang: "java",
    code: `package com.customer360.spark.pipeline;

import com.customer360.spark.aqe.AQEMonitor;
import com.customer360.spark.generator.DataGenerator;
import com.customer360.spark.join.JoinStrategySelector;
import com.customer360.spark.partition.PartitionStrategy;
import com.customer360.spark.util.MetricsLogger;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Master pipeline orchestrator.
 *
 * Execution Phases & SLA budget
 * ──────────────────────────────
 *  Phase 0 — Setup         (≈ 0 min)  : SparkSession, Hive database
 *  Phase 1 — Generate      (≈ 2 min)  : Synthetic data for all 6 tables
 *  Phase 2 — Persist       (≈ 3 min)  : Write bucketed Parquet tables
 *  Phase 3 — Join DAG      (≈ 18 min) : 6-way join with AQE monitoring
 *  Phase 4 — Aggregate     (≈ 4 min)  : Customer 360 KPI computation
 *  Phase 5 — Output        (≈ 3 min)  : Write result + print metrics
 *
 * 6-way Join DAG (left-to-right dependency):
 *
 *  [customers] ──SMJ──► [+orders] ──BHJ──► [+products]
 *                                                │
 *                                           ──SMJ/AQE──► [+web_events]
 *                                                │
 *                                           ──SHJ──► [+support_tickets]
 *                                                │
 *                                           ──SMJ─(bucket)──► [+loyalty_rewards]
 *                                                │
 *                                           [CUSTOMER_360_GOLD]
 */
public class Customer360Pipeline {

    private static final Logger LOG = LoggerFactory.getLogger(Customer360Pipeline.class);

    private final SparkSession  spark;
    private final MetricsLogger metrics;
    private final AQEMonitor    aqeMonitor;
    private final DataGenerator generator;

    public Customer360Pipeline(SparkSession spark, MetricsLogger metrics) {
        this.spark      = spark;
        this.metrics    = metrics;
        this.aqeMonitor = new AQEMonitor(spark);
        this.generator  = new DataGenerator(spark);
    }

    public void run() throws Exception {

        // ── Phase 0: Setup ──────────────────────────────────────────────
        LOG.info("Phase 0 — Setting up Hive database …");
        spark.sql("CREATE DATABASE IF NOT EXISTS c360");
        spark.sql("USE c360");

        // ── Phase 1: Generate synthetic data ────────────────────────────
        LOG.info("Phase 1 — Generating synthetic data …");
        metrics.startPhase("GENERATE");

        Dataset<Row> customers      = generator.generateCustomers().cache();
        Dataset<Row> orders         = generator.generateOrders();
        Dataset<Row> products       = generator.generateProducts().cache();
        Dataset<Row> webEvents      = generator.generateWebEvents();
        Dataset<Row> supportTickets = generator.generateSupportTickets();
        Dataset<Row> loyaltyRewards = generator.generateLoyaltyRewards();

        // Force materialisation of small tables into memory
        LOG.info("Customers count: {}",  customers.count());
        LOG.info("Products count: {}",   products.count());

        metrics.endPhase("GENERATE");

        // ── Phase 2: Write bucketed tables ──────────────────────────────
        LOG.info("Phase 2 — Persisting bucketed tables …");
        metrics.startPhase("BUCKET_WRITE");

        PartitionStrategy.persistCustomers(customers);
        PartitionStrategy.persistOrders(orders);
        PartitionStrategy.persistWebEvents(webEvents);
        PartitionStrategy.persistSupportTickets(supportTickets);
        PartitionStrategy.persistLoyaltyRewards(loyaltyRewards);

        // Products is small — store as plain Parquet (will be broadcast)
        products.write()
            .mode(SaveMode.Overwrite)
            .partitionBy("category")
            .parquet("spark-warehouse/c360/products");

        metrics.endPhase("BUCKET_WRITE");

        // ── Phase 3: Read back bucketed tables ──────────────────────────
        LOG.info("Phase 3 — Building 6-way join DAG …");
        metrics.startPhase("JOIN_DAG");

        Dataset<Row> custBucketed   = spark.table("c360.customers_bucketed");
        Dataset<Row> ordersBucketed = spark.table("c360.orders_bucketed");
        Dataset<Row> webBucketed    = spark.table("c360.web_events_bucketed");
        Dataset<Row> ticketsBucketed= spark.table("c360.support_tickets_bucketed");
        Dataset<Row> loyaltyBucketed= spark.table("c360.loyalty_rewards_bucketed");
        Dataset<Row> prodsDf        = spark.read()
                                          .parquet("spark-warehouse/c360/products");

        // ── HOP 1: customers × orders (SMJ, bucketed → no shuffle) ──────
        metrics.startPhase("HOP1_CUSTOMERS_ORDERS");
        Dataset<Row> hop1 = JoinStrategySelector.joinCustomersOrders(custBucketed, ordersBucketed);
        aqeMonitor.explainPlan(hop1, "HOP1 customers×orders");
        hop1 = hop1.checkpoint(); // materialise before next hop to reset lineage
        metrics.endPhase("HOP1_CUSTOMERS_ORDERS");
        LOG.info("HOP 1 complete — rows: {}", hop1.count());

        // ── HOP 2: + products (BHJ — forced broadcast) ──────────────────
        metrics.startPhase("HOP2_PRODUCTS");
        Dataset<Row> hop2 = JoinStrategySelector.joinProducts(hop1, prodsDf);
        aqeMonitor.explainPlan(hop2, "HOP2 +products");
        metrics.endPhase("HOP2_PRODUCTS");

        // ── HOP 3: + web_events (SMJ + AQE skew handling) ───────────────
        metrics.startPhase("HOP3_WEB_EVENTS");
        Dataset<Row> hop3 = JoinStrategySelector.joinWebEvents(hop2, webBucketed);
        aqeMonitor.captureAQEDecisions(hop3, "HOP3 web_events");
        hop3 = hop3.checkpoint();
        metrics.endPhase("HOP3_WEB_EVENTS");
        LOG.info("HOP 3 complete");

        // ── HOP 4: + support_tickets (SHJ after AQE coalesce) ───────────
        metrics.startPhase("HOP4_SUPPORT");
        Dataset<Row> hop4 = JoinStrategySelector.joinSupportTickets(hop3, ticketsBucketed);
        aqeMonitor.explainPlan(hop4, "HOP4 +support_tickets");
        metrics.endPhase("HOP4_SUPPORT");

        // ── HOP 5: + loyalty_rewards (SMJ bucket-to-bucket, no shuffle) ─
        metrics.startPhase("HOP5_LOYALTY");
        Dataset<Row> hop5 = JoinStrategySelector.joinLoyaltyRewards(hop4, loyaltyBucketed);
        aqeMonitor.explainPlan(hop5, "HOP5 +loyalty_rewards");
        metrics.endPhase("HOP5_LOYALTY");

        metrics.endPhase("JOIN_DAG");

        // ── Phase 4: Aggregation — Customer 360 Gold Layer ──────────────
        LOG.info("Phase 4 — Computing Customer 360 KPIs …");
        metrics.startPhase("AGGREGATE");

        Dataset<Row> customer360Gold = hop5
            .groupBy(
                "customer_id", "first_name", "last_name", "email",
                "country_code", "segment", "age_bucket",
                "lifetime_value", "is_active", "loyalty_tier"
            ).agg(
                count("order_id").alias("total_orders"),
                sum("order_amount").alias("total_revenue"),
                avg("order_amount").alias("avg_order_value"),
                countDistinct("prd_category").alias("distinct_categories"),
                sum("web_purchases").alias("total_web_purchases"),
                avg("avg_session_secs").alias("avg_session_secs"),
                sum("total_events").alias("total_web_events"),
                avg("avg_satisfaction").alias("csat_score"),
                sum("critical_tickets").alias("critical_tickets"),
                sum("points_balance").alias("loyalty_balance"),
                max("total_points_earned").alias("max_points_earned"),

                // Derived KPIs
                (sum("order_amount").divide(
                    when(count("order_id").equalTo(0), 1).otherwise(count("order_id")))
                ).alias("clv_per_order"),

                when(col("loyalty_tier").isin("GOLD","DIAMOND")
                     .and(col("segment").equalTo("PLATINUM")), lit("VIP"))
                .when(col("lifetime_value").gt(10000), lit("HIGH_VALUE"))
                .otherwise(lit("STANDARD"))
                .alias("c360_classification")
            )
            // Final filter: only active customers with at least 1 order
            .filter(col("is_active").equalTo(true).and(col("total_orders").gt(0)));

        metrics.endPhase("AGGREGATE");

        // ── Phase 5: Write Gold Layer + Report ──────────────────────────
        LOG.info("Phase 5 — Writing Customer 360 Gold layer …");
        metrics.startPhase("OUTPUT");

        customer360Gold.write()
            .mode(SaveMode.Overwrite)
            .partitionBy("segment", "country_code")
            .parquet("spark-warehouse/c360/customer_360_gold");

        // Quick sanity report
        LOG.info("=== Customer 360 Gold Layer Summary ===");
        customer360Gold
            .groupBy("segment", "c360_classification")
            .agg(
                count("customer_id").alias("customer_count"),
                avg("total_revenue").alias("avg_revenue"),
                avg("csat_score").alias("avg_csat"),
                avg("loyalty_balance").alias("avg_loyalty_pts")
            )
            .orderBy(desc("avg_revenue"))
            .show(20, false);

        metrics.endPhase("OUTPUT");
        aqeMonitor.printAQESummary();
        metrics.printSummary();
    }
}
`,
  },

  // ─── 8. Metrics Logger ────────────────────────────────────────────────────
  "MetricsLogger.java": {
    label: "MetricsLogger.java",
    lang: "java",
    code: `package com.customer360.spark.util;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lightweight phase-level metrics logger.
 * Tracks wall-clock time per pipeline phase and prints a SLA report.
 */
public class MetricsLogger {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsLogger.class);

    private final SparkSession spark;
    private final Map<String, Instant> starts = new ConcurrentHashMap<>();
    private final List<PhaseRecord>    records = Collections.synchronizedList(new ArrayList<>());

    private static final long SLA_SECONDS = 1800; // 30 minutes

    public MetricsLogger(SparkSession spark) { this.spark = spark; }

    public void startPhase(String name) {
        starts.put(name, Instant.now());
        LOG.info("▶  [{}] started", name);
    }

    public void endPhase(String name) {
        Instant start = starts.remove(name);
        if (start == null) return;
        long secs = Duration.between(start, Instant.now()).getSeconds();
        records.add(new PhaseRecord(name, secs));
        LOG.info("✔  [{}] completed in {} s", name, secs);
    }

    public void printSummary() {
        long total = records.stream().mapToLong(r -> r.durationSecs).sum();
        boolean withinSla = total <= SLA_SECONDS;

        LOG.info("");
        LOG.info("╔══════════════════════════════════════════════════════╗");
        LOG.info("║         Customer 360 Pipeline — Phase Report         ║");
        LOG.info("╠══════════════════╦═══════════════╦═══════════════════╣");
        LOG.info("║ Phase            ║ Duration (s)  ║ SLA Contribution  ║");
        LOG.info("╠══════════════════╬═══════════════╬═══════════════════╣");

        for (PhaseRecord r : records) {
            double pct = total > 0 ? (r.durationSecs * 100.0 / SLA_SECONDS) : 0;
            LOG.info("║ {:<16} ║ {:>13} ║ {:>16.1f}%  ║",
                truncate(r.phase, 16), r.durationSecs, pct);
        }

        LOG.info("╠══════════════════╬═══════════════╬═══════════════════╣");
        LOG.info("║ TOTAL            ║ {:>13} ║ {:>16.1f}%  ║",
            total, total * 100.0 / SLA_SECONDS);
        LOG.info("╠══════════════════╩═══════════════╩═══════════════════╣");
        LOG.info("║ SLA (30 min = 1800 s)  →  {}                   ║",
            withinSla ? "✅ WITHIN SLA" : "❌ SLA BREACHED");
        LOG.info("╚══════════════════════════════════════════════════════╝");

        // Print Spark UI URL
        spark.sparkContext().uiWebUrl().foreach(url ->
            LOG.info("Spark UI: {}", url)
        );
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max - 1) + "…";
    }

    record PhaseRecord(String phase, long durationSecs) {}
}
`,
  },

  // ─── 9. BucketingVsPartitioning Demo ─────────────────────────────────────
  "BucketingVsPartitioningDemo.java": {
    label: "BucketingVsPartitioningDemo.java",
    lang: "java",
    code: `package com.customer360.spark.demo;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║  Standalone Demo — Bucketing vs Partitioning Tradeoffs                  ║
 * ║  Run this class independently to see the difference in query plans.     ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 *
 * TEST MATRIX
 * ───────────
 *  Test 1  Partitioned-only  →  two full shuffles in SMJ plan
 *  Test 2  Bucketed (same key, same count)  →  ZERO shuffles in SMJ plan
 *  Test 3  Bucketed (wrong key)  →  shuffle re-introduced (misaligned bucket)
 *  Test 4  Bucketed (different counts: 512 vs 128)  →  shuffle on larger side
 *  Test 5  Small table  →  AQE auto-broadcast (no hint required)
 *
 * Run with: -Ddemo.rows=500000 to control data size
 */
public class BucketingVsPartitioningDemo {

    private static final Logger LOG = LoggerFactory.getLogger(BucketingVsPartitioningDemo.class);
    private static final long ROWS = Long.parseLong(System.getProperty("demo.rows", "500000"));

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("BucketingVsPartitioning-Demo")
            .master("local[*]")
            .config("spark.sql.adaptive.enabled",               "true")
            .config("spark.sql.autoBroadcastJoinThreshold",     "-1")  // disable for tests 1-4
            .config("spark.sql.shuffle.partitions",             "200")
            .enableHiveSupport()
            .getOrCreate();

        spark.sql("CREATE DATABASE IF NOT EXISTS bvp_demo");
        spark.sql("USE bvp_demo");

        runTest1_PartitionedOnly(spark);
        runTest2_BucketedSameKeyCount(spark);
        runTest3_BucketedWrongKey(spark);
        runTest4_BucketedDifferentCounts(spark);
        runTest5_AQEAutoBroadcast(spark);

        spark.stop();
    }

    // ── Test 1: Partition-only — full shuffle ─────────────────────────────
    static void runTest1_PartitionedOnly(SparkSession spark) {
        LOG.info("\\n═══ TEST 1: Partition-only (expect 2 Exchange nodes) ═══");

        Dataset<Row> a = makeData(spark, ROWS, "a_partitioned");
        Dataset<Row> b = makeData(spark, ROWS / 10, "b_partitioned");

        // Write as partition-by-date only — no bucketing
        a.write().mode(SaveMode.Overwrite).partitionBy("ds").parquet("bvp-warehouse/a_part");
        b.write().mode(SaveMode.Overwrite).partitionBy("ds").parquet("bvp-warehouse/b_part");

        Dataset<Row> aPart = spark.read().parquet("bvp-warehouse/a_part");
        Dataset<Row> bPart = spark.read().parquet("bvp-warehouse/b_part");

        Dataset<Row> result = aPart.join(bPart, "customer_id");

        LOG.info("Plan (look for 2× Exchange):");
        result.explain("formatted");
        // Expected: two Exchange (hashpartitioning) nodes before SortMergeJoin
    }

    // ── Test 2: Bucketed same key + same count — ZERO shuffles ───────────
    static void runTest2_BucketedSameKeyCount(SparkSession spark) {
        LOG.info("\\n═══ TEST 2: Bucketed same key + same count (expect 0 Exchange) ═══");

        Dataset<Row> a = makeData(spark, ROWS, "a_bucketed");
        Dataset<Row> b = makeData(spark, ROWS / 10, "b_bucketed");

        a.write().mode(SaveMode.Overwrite)
            .bucketBy(128, "customer_id").sortBy("customer_id")
            .saveAsTable("bvp_demo.a_bucketed");

        b.write().mode(SaveMode.Overwrite)
            .bucketBy(128, "customer_id").sortBy("customer_id")
            .saveAsTable("bvp_demo.b_bucketed");

        Dataset<Row> aBuck = spark.table("bvp_demo.a_bucketed");
        Dataset<Row> bBuck = spark.table("bvp_demo.b_bucketed");

        Dataset<Row> result = aBuck.join(bBuck, "customer_id");

        LOG.info("Plan (look for 0× Exchange — SortMergeJoin directly):");
        result.explain("formatted");
        // Expected: SortMergeJoin with NO Exchange nodes — pure bucket merge
    }

    // ── Test 3: Bucketed but wrong join key — shuffle reintroduced ────────
    static void runTest3_BucketedWrongKey(SparkSession spark) {
        LOG.info("\\n═══ TEST 3: Bucketed on customer_id but joining on product_id (expect Exchange) ═══");

        Dataset<Row> aBuck = spark.table("bvp_demo.a_bucketed");
        Dataset<Row> bBuck = spark.table("bvp_demo.b_bucketed");

        // Join on a column different from the bucket key → Exchange re-added
        Dataset<Row> result = aBuck.join(bBuck,
            aBuck.col("product_id").equalTo(bBuck.col("product_id")));

        LOG.info("Plan (Exchange reintroduced because join key ≠ bucket key):");
        result.explain("formatted");
    }

    // ── Test 4: Different bucket counts — shuffle on one side ─────────────
    static void runTest4_BucketedDifferentCounts(SparkSession spark) {
        LOG.info("\\n═══ TEST 4: a=512 buckets vs b=128 buckets (expect Exchange on larger side) ═══");

        Dataset<Row> a = makeData(spark, ROWS, "a512");
        Dataset<Row> b = makeData(spark, ROWS / 10, "b128");

        a.write().mode(SaveMode.Overwrite)
            .bucketBy(512, "customer_id").sortBy("customer_id")
            .saveAsTable("bvp_demo.a_512");

        b.write().mode(SaveMode.Overwrite)
            .bucketBy(128, "customer_id").sortBy("customer_id")  // 4× smaller
            .saveAsTable("bvp_demo.b_128");

        Dataset<Row> result = spark.table("bvp_demo.a_512")
            .join(spark.table("bvp_demo.b_128"), "customer_id");

        LOG.info("Plan (Exchange on the 512-bucket side OR on b depending on Spark version):");
        result.explain("formatted");
        // Spark 3.x: may still avoid Exchange if counts are multiples; otherwise one side shuffled
    }

    // ── Test 5: AQE auto-broadcast after filter (no hint) ─────────────────
    static void runTest5_AQEAutoBroadcast(SparkSession spark) {
        LOG.info("\\n═══ TEST 5: AQE converts SMJ → BHJ at runtime (re-enable broadcast threshold) ═══");

        // Re-enable broadcast for this test
        spark.conf().set("spark.sql.autoBroadcastJoinThreshold",         "104857600"); // 100 MB
        spark.conf().set("spark.sql.adaptive.autoBroadcastJoinThreshold","209715200"); // 200 MB

        Dataset<Row> large = makeData(spark, ROWS, "large_tbl");
        Dataset<Row> small = makeData(spark, ROWS / 10, "small_tbl")
            .filter(col("customer_id").lt(1000)); // filter → ~1000 rows — tiny!

        // Static plan: SMJ (Spark doesn't know filter result is tiny)
        // AQE plan:    BHJ (after shuffle Spark sees actual size < 200 MB)
        Dataset<Row> result = large.join(small, "customer_id");

        LOG.info("Plan after AQE reoptimisation (expect BroadcastHashJoin):");
        result.explain("formatted");
    }

    // ── Helper: generate synthetic data ───────────────────────────────────
    private static Dataset<Row> makeData(SparkSession spark, long rows, String tag) {
        return spark.range(1, rows + 1).toDF("id")
            .withColumn("customer_id",  expr("cast(rand() * " + rows + " + 1 as long)"))
            .withColumn("product_id",   expr("cast(rand() * 10000 + 1 as long)"))
            .withColumn("amount",       expr("round(rand() * 5000, 2)"))
            .withColumn("ds",           expr("date_sub(current_date(), cast(rand()*30 as int))"))
            .withColumn("tag",          lit(tag));
    }
}
`,
  },

  // ─── 10. pom.xml ─────────────────────────────────────────────────────────
  "pom.xml": {
    label: "pom.xml",
    lang: "xml",
    code: `<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">

    <modelVersion>4.0.0</modelVersion>
    <groupId>com.customer360</groupId>
    <artifactId>customer360-spark</artifactId>
    <version>1.0-SNAPSHOT</version>
    <packaging>jar</packaging>
    <name>Customer 360 — 6-Way Join Spark Application</name>

    <properties>
        <java.version>17</java.version>
        <spark.version>3.5.1</spark.version>
        <scala.binary.version>2.13</scala.binary.version>
        <hadoop.version>3.3.6</hadoop.version>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>

        <!-- ── Apache Spark Core ──────────────────────────────────── -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_\${scala.binary.version}</artifactId>
            <version>\${spark.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- ── Spark SQL (DataFrame / Dataset API) ──────────────── -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_\${scala.binary.version}</artifactId>
            <version>\${spark.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- ── Spark Hive (for bucketed table metadata) ──────────── -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-hive_\${scala.binary.version}</artifactId>
            <version>\${spark.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- ── Hadoop Client ─────────────────────────────────────── -->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>\${hadoop.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- ── Logging ───────────────────────────────────────────── -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>2.0.9</version>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>1.4.14</version>
        </dependency>

        <!-- ── Testing ───────────────────────────────────────────── -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter-engine</artifactId>
            <version>5.10.2</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_\${scala.binary.version}</artifactId>
            <version>\${spark.version}</version>
            <classifier>tests</classifier>
            <scope>test</scope>
        </dependency>

    </dependencies>

    <build>
        <plugins>

            <!-- Maven Compiler (Java 17 with preview disabled) -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.12.1</version>
                <configuration>
                    <source>17</source>
                    <target>17</target>
                    <compilerArgs>
                        <arg>--add-exports=java.base/sun.nio.ch=ALL-UNNAMED</arg>
                        <arg>--add-opens=java.base/java.nio=ALL-UNNAMED</arg>
                        <arg>--add-opens=java.base/sun.nio.ch=ALL-UNNAMED</arg>
                    </compilerArgs>
                </configuration>
            </plugin>

            <!-- Shade plugin — fat JAR for spark-submit -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.5.2</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals><goal>shade</goal></goals>
                        <configuration>
                            <shadedArtifactAttached>true</shadedArtifactAttached>
                            <shadedClassifierName>uber</shadedClassifierName>
                            <transformers>
                                <transformer implementation=
                                 "org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>com.customer360.spark.Customer360App</mainClass>
                                </transformer>
                                <transformer implementation=
                                 "org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                            </transformers>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                        </configuration>
                    </execution>
                </executions>
            </plugin>

            <!-- Surefire for JUnit 5 tests -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.2.5</version>
                <configuration>
                    <useModulePath>false</useModulePath>
                    <argLine>
                        --add-opens java.base/java.lang=ALL-UNNAMED
                        --add-opens java.base/java.util=ALL-UNNAMED
                        --add-opens java.base/java.io=ALL-UNNAMED
                    </argLine>
                </configuration>
            </plugin>

        </plugins>
    </build>
</project>
`,
  },

  // ─── 11. Run / IntelliJ guide ─────────────────────────────────────────────
  "README_IntelliJ.md": {
    label: "README_IntelliJ.md",
    lang: "markdown",
    code: `# Customer 360 — 6-Way Join Model (Java 17 + Spark 3.5)

## IntelliJ IDEA Setup (step-by-step)

### Prerequisites
| Tool       | Version  |
|------------|----------|
| JDK        | 17 (e.g., Amazon Corretto 17 or Eclipse Temurin 17) |
| Maven      | 3.9+     |
| Spark      | 3.5.1 (no local install needed — Maven downloads)   |
| IntelliJ   | 2023.3+  |
| RAM        | ≥ 16 GB (driver + executors run in-process locally) |
| Disk       | ≥ 20 GB  |

### 1. Clone / create project structure
\`\`\`
customer360-spark/
├── pom.xml
└── src/
    └── main/
        └── java/
            └── com/customer360/spark/
                ├── Customer360App.java
                ├── aqe/
                │   └── AQEMonitor.java
                ├── config/
                │   └── SparkSessionConfig.java
                ├── demo/
                │   └── BucketingVsPartitioningDemo.java
                ├── generator/
                │   └── DataGenerator.java
                ├── join/
                │   └── JoinStrategySelector.java
                ├── partition/
                │   └── PartitionStrategy.java
                ├── pipeline/
                │   └── Customer360Pipeline.java
                └── util/
                    └── MetricsLogger.java
\`\`\`

### 2. Import into IntelliJ
1. **File → Open** → select the \`customer360-spark\` folder
2. IntelliJ detects \`pom.xml\` → click **Trust Project**
3. Wait for Maven sync (downloads ~800 MB of Spark JARs)

### 3. Configure Run / Debug
1. **Run → Edit Configurations → + → Application**
2. **Main class**: \`com.customer360.spark.Customer360App\`
3. **VM options** (required for Spark on Java 17):
\`\`\`
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.lang.invoke=ALL-UNNAMED
--add-opens java.base/java.lang.reflect=ALL-UNNAMED
--add-opens java.base/java.io=ALL-UNNAMED
--add-opens java.base/java.net=ALL-UNNAMED
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/java.util=ALL-UNNAMED
--add-opens java.base/java.util.concurrent=ALL-UNNAMED
--add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
--add-opens java.base/sun.nio.cs=ALL-UNNAMED
--add-opens java.base/sun.security.action=ALL-UNNAMED
--add-opens java.base/sun.util.calendar=ALL-UNNAMED
-Dlog4j2.formatMsgNoLookups=true
-Ddemo.scale=0.001
\`\`\`
4. **Environment variables**:
\`\`\`
SPARK_LOCAL_IP=127.0.0.1
HADOOP_USER_NAME=spark
\`\`\`

### 4. Local Scale vs Production Scale
| JVM property        | Value   | Customers  | Orders     | Run time  |
|---------------------|---------|------------|------------|-----------|
| \`-Ddemo.scale=0.001\`| default | 500 K      | 2 M        | ~3 min    |
| \`-Ddemo.scale=0.01\` | medium  | 5 M        | 20 M       | ~15 min   |
| \`-Ddemo.scale=0.1\`  | large   | 50 M       | 200 M      | ~90 min   |
| \`-Ddemo.scale=1.0\`  | prod    | 500 M      | 2 B        | ~25-30 min|

### 5. Running the Bucketing Demo standalone
1. **Run → Edit Configurations → + → Application**
2. **Main class**: \`com.customer360.spark.demo.BucketingVsPartitioningDemo\`
3. Add same VM options as above + \`-Ddemo.rows=500000\`

### 6. Spark UI
While the job runs, open: **http://localhost:4040**
- **SQL tab** → look for \`AdaptiveSparkPlan isFinalPlan=true\`
- **Stages tab** → compare shuffle read/write before and after bucketing
- **Storage tab** → cached DataFrames (customers, products)

### 7. spark-submit (cluster mode)
\`\`\`bash
mvn clean package -DskipTests

spark-submit \\
  --master yarn \\
  --deploy-mode cluster \\
  --num-executors 20 \\
  --executor-cores 16 \\
  --executor-memory 48g \\
  --driver-memory 8g \\
  --conf spark.sql.adaptive.enabled=true \\
  --conf spark.sql.adaptive.skewJoin.enabled=true \\
  --conf spark.sql.autoBroadcastJoinThreshold=104857600 \\
  --conf spark.dynamicAllocation.enabled=true \\
  --conf spark.dynamicAllocation.maxExecutors=40 \\
  --class com.customer360.spark.Customer360App \\
  target/customer360-spark-1.0-SNAPSHOT-uber.jar
\`\`\`

### 8. Expected output (sample)
\`\`\`
╔══════════════════════════════════════════════════════╗
║         Customer 360 Pipeline — Phase Report         ║
╠══════════════════╦═══════════════╦═══════════════════╣
║ Phase            ║ Duration (s)  ║ SLA Contribution  ║
╠══════════════════╬═══════════════╬═══════════════════╣
║ GENERATE         ║           118 ║             6.6%  ║
║ BUCKET_WRITE     ║           187 ║            10.4%  ║
║ HOP1_CUS_ORDERS  ║           312 ║            17.3%  ║
║ HOP2_PRODUCTS    ║            48 ║             2.7%  ║
║ HOP3_WEB_EVENTS  ║           398 ║            22.1%  ║
║ HOP4_SUPPORT     ║           201 ║            11.2%  ║
║ HOP5_LOYALTY     ║           189 ║            10.5%  ║
║ AGGREGATE        ║           224 ║            12.4%  ║
║ OUTPUT           ║           103 ║             5.7%  ║
╠══════════════════╬═══════════════╬═══════════════════╣
║ TOTAL            ║          1780 ║            98.9%  ║
╠══════════════════╩═══════════════╩═══════════════════╣
║ SLA (30 min = 1800 s)  →  ✅ WITHIN SLA              ║
╚══════════════════════════════════════════════════════╝
\`\`\`
`,
  },
};
