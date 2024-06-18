import datetime
import pendulum

from airflow.decorators import dag, task, task_group # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore

@dag(
    dag_id="batch_process",
    schedule_interval = '@hourly',
    start_date=pendulum.datetime(2024, 6, 14, 17, 00, tz="Asia/Shanghai"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessOrders():

    create_temp_processed_data_table = PostgresOperator(
        task_id="create_temp_processed_data_table",
        postgres_conn_id="batch_process",
        sql="""
            DROP TABLE IF EXISTS temp_processed_data;
            CREATE TABLE temp_processed_data (
                "Category" TEXT,
                "Total_Revenue" REAL,
                "day_of_month" INTEGER,
                "hour_of_day" INTEGER
            );""",
        )

    @task_group(group_id = "Get-data-of-products")
    def product_data():
        create_products_table = PostgresOperator(
            task_id="create_products_table",
            postgres_conn_id="batch_process",
            sql="""
                DROP TABLE IF EXISTS products;
                CREATE TABLE products (
                    "ProductID" INTEGER,
                    "Name" TEXT,
                    "Description" TEXT,
                    "Price" NUMERIC(10,2),
                    "Category" TEXT
                );""",
        )

        @task(task_id = "Create-table-for-list-of-products")
        def get_products():
            data_path = "/opt/airflow/dags/data/products.csv"

            # Load data from CSV file into temporary SQL table
            with open(data_path, "r") as file:
                postgres_hook = PostgresHook(postgres_conn_id="batch_process")
                conn = postgres_hook.get_conn()
                cur = conn.cursor()
                # Copy data from CSV file to temporary SQL table
                cur.copy_expert(
                    "COPY products FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                    file,
                )
                conn.commit()
        
        create_products_table >> get_products()

    @task_group(group_id = "Get-latest-data-of-item-orders")
    def latest_order_items_data():
        create_temp_orders_table = PostgresOperator(
            task_id="create_temp_orders_table",
            postgres_conn_id="batch_process",
            sql="""
                DROP TABLE IF EXISTS temp_order_items;
                CREATE TABLE temp_order_items (
                    "product_id" INTEGER,
                    "quantity" INTEGER,
                    "price_per_unit" REAL,
                    "timestamp" TEXT
                );""",
        )

        @task(task_id = "Copy-latest-orders-data-into-temporary-orders-table")    
        def copy_latest_order_items():
            postgres_hook = PostgresHook(postgres_conn_id="batch_process")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            # # empty the table first 
            # cur.execute(
            #     """
            #     TRUNCATE TABLE temp_order_items;
            #     """
            #     )
            # Copy relevant orders information into a temporary table
            query = """
                    INSERT INTO temp_order_items 
                    SELECT * 
                    FROM order_items
                    WHERE EXTRACT(HOUR FROM TO_TIMESTAMP(timestamp, 'YYYY-MM-DD-HH24-MI-SS'))= EXTRACT(HOUR FROM NOW() AT TIME ZONE 'Asia/Shanghai')-1;

                    """
            cur.execute(query)  
            conn.commit()
            
        @task(task_id = "Transform-temporary-orders-table")
        def transform_temp_order_items():
            postgres_hook = PostgresHook(postgres_conn_id="batch_process")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            # Add new columns for day and hour
            query = """
                    ALTER TABLE temp_order_items
                    ADD COLUMN day_of_month INT,
                    ADD COLUMN hour_of_day INT;
                    """
            
            cur.execute(query)
            # Update the table to populate the day and hour columns
            query = """
                    UPDATE temp_order_items
                    SET 
                        day_of_month = DATE_PART('day', TO_TIMESTAMP(timestamp, 'YYYY-MM-DD-HH24-MI-SS')),
                        hour_of_day = DATE_PART('hour', TO_TIMESTAMP(timestamp, 'YYYY-MM-DD-HH24-MI-SS'));
                    """
            cur.execute(query)
            # Drop the timestamp column
            query = """
                    ALTER TABLE temp_order_items
                    DROP COLUMN timestamp;
                    """
            cur.execute(query)
            conn.commit()

        create_temp_orders_table >> copy_latest_order_items() >> transform_temp_order_items()

    @task_group(group_id = "Aggregate-data-and-update-hour-sales-table")
    def aggregate_update():

        @task(task_id = "Copy-aggregate-sales-data-into-temporary-table")
        def copy_joint_aggregate_data():
            postgres_hook = PostgresHook(postgres_conn_id="batch_process")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            # Join the products table and temp_order_items table on Product ID,
            # then groupby the category. Copy into temp_processed_data 
            query = """
                    INSERT INTO temp_processed_data
                    SELECT 
                        p."Category",
                        SUM(t.quantity * t.price_per_unit) AS Total_Revenue,
                        t."day_of_month",
                        t."hour_of_day"
                    FROM 
                        products p
                    JOIN 
                        temp_order_items t
                    ON 
                        p."ProductID" = t.product_id
                    GROUP BY 
                        p."Category",
                        t."day_of_month",
                        t."hour_of_day";
                    """
            cur.execute(query)
            conn.commit()

        @task(task_id = "Update-hourly-sales-table")
        def update_hourly_sales():
            postgres_hook = PostgresHook(postgres_conn_id="batch_process")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            # Pivot the temporary sales data table and insert into hourly_sales_data table.
            query = """
                    INSERT INTO hourly_sales_data ("hour_of_day", "Kitchen", "Sports", "Tools", "Fashion", "Widgets", "Electronics", "Gadgets", "Home", "Office")
                    SELECT
                        t."hour_of_day",
                        MAX(CASE WHEN t."Category" = 'Kitchen' THEN t."Total_Revenue" ELSE 0 END) AS Kitchen,
                        MAX(CASE WHEN t."Category" = 'Sports' THEN t."Total_Revenue" ELSE 0 END) AS Sports,
                        MAX(CASE WHEN t."Category" = 'Tools' THEN t."Total_Revenue" ELSE 0 END) AS Tools,
                        MAX(CASE WHEN t."Category" = 'Fashion' THEN t."Total_Revenue" ELSE 0 END) AS Fashion,
                        MAX(CASE WHEN t."Category" = 'Widgets' THEN t."Total_Revenue" ELSE 0 END) AS Widgets,
                        MAX(CASE WHEN t."Category" = 'Electronics' THEN t."Total_Revenue" ELSE 0 END) AS Electronics,
                        MAX(CASE WHEN t."Category" = 'Gadgets' THEN t."Total_Revenue" ELSE 0 END) AS Gadgets,
                        MAX(CASE WHEN t."Category" = 'Home' THEN t."Total_Revenue" ELSE 0 END) AS Home,
                        MAX(CASE WHEN t."Category" = 'Office' THEN t."Total_Revenue" ELSE 0 END) AS Office
                    FROM
                        temp_processed_data t
                    GROUP BY
                        t."hour_of_day";
                    """
            cur.execute(query)
            conn.commit()


        copy_joint_aggregate_data() >> update_hourly_sales()

    create_temp_processed_data_table >> [product_data(), latest_order_items_data()] >> aggregate_update()

dag = ProcessOrders()



# CREATE TABLE hourly_sales_data (
# "hour_of_day" INTEGER,
# "Kitchen" REAL,
# "Sports" REAL,
# "Tools" REAL,
# "Fashion" REAL,
# "Widgets" REAL,
# "Electronics" REAL,
# "Gadgets" REAL,
# "Home" REAL,
# "Office" REAL);
# CREATE TABLE
