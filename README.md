import psycopg2
from psycopg2 import sql
import uuid
from datetime import datetime
import os

# Define environment variables or placeholders for your database connection
# It is highly recommended to use environment variables in a real-world scenario.
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "finance_db")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "mysecretpassword")
DB_PORT = os.environ.get("DB_PORT", "5432")

def connect_db():
    """
    Establishes and returns a connection to the PostgreSQL database.
    Handles potential connection errors.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        return None

def create_table(conn):
    """
    Creates the 'transactions' table if it does not already exist.
    This is a one-time setup function.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id VARCHAR(255) PRIMARY KEY,
        transaction_date DATE NOT NULL,
        description TEXT,
        amount DECIMAL(10, 2) NOT NULL,
        type VARCHAR(20) NOT NULL CHECK (type IN ('Revenue', 'Expense'))
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
        conn.commit()
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        conn.rollback()

def insert_transaction(conn, description, amount, type, transaction_date=None):
    """
    Inserts a new transaction record into the database.
    Generates a unique ID and uses today's date if not provided.
    
    :param conn: The database connection object.
    :param description: A text description of the transaction.
    :param amount: The monetary amount of the transaction.
    :param type: The type of transaction ('Revenue' or 'Expense').
    :param transaction_date: The date of the transaction (optional).
    """
    transaction_id = str(uuid.uuid4())
    if transaction_date is None:
        transaction_date = datetime.now().date()

    insert_query = sql.SQL("""
    INSERT INTO transactions (transaction_id, transaction_date, description, amount, type)
    VALUES (%s, %s, %s, %s, %s)
    """)
    
    try:
        with conn.cursor() as cur:
            cur.execute(insert_query, (transaction_id, transaction_date, description, amount, type))
        conn.commit()
        return True, "Transaction added successfully!"
    except psycopg2.Error as e:
        print(f"Error inserting transaction: {e}")
        conn.rollback()
        return False, f"Error: {e}"

def get_transactions(conn, filter_type=None, sort_by=None, sort_order='ASC'):
    """
    Fetches all transactions from the database with optional filtering and sorting.
    
    :param conn: The database connection object.
    :param filter_type: 'Revenue', 'Expense', or None for all.
    :param sort_by: 'amount' or 'transaction_date' to sort by.
    :param sort_order: 'ASC' or 'DESC'.
    :return: A list of dictionaries, where each dictionary represents a transaction.
    """
    # Base query
    query = sql.SQL("SELECT transaction_id, transaction_date, description, amount, type FROM transactions")
    params = []
    
    # Add filtering if a type is selected
    if filter_type and filter_type != 'All':
        query += sql.SQL(" WHERE type = %s")
        params.append(filter_type)
        
    # Add sorting if a sort column is selected
    if sort_by:
        order_clause = sql.SQL(" ORDER BY {} {}").format(
            sql.Identifier(sort_by),
            sql.Literal(sort_order)
        )
        query += order_clause
        
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query, params)
            transactions = [dict(row) for row in cur.fetchall()]
        return transactions
    except psycopg2.Error as e:
        print(f"Error fetching transactions: {e}")
        return []

def get_aggregates(conn):
    """
    Calculates and returns key aggregate metrics.
    
    :param conn: The database connection object.
    :return: A dictionary of aggregate values.
    """
    aggregate_query = """
    SELECT
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN type = 'Revenue' THEN amount ELSE 0 END) AS total_revenue,
        SUM(CASE WHEN type = 'Expense' THEN amount ELSE 0 END) AS total_expenses,
        SUM(CASE WHEN type = 'Revenue' THEN amount ELSE -amount END) AS net_income
    FROM transactions;
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(aggregate_query)
            result = cur.fetchone()
            
            # Handle the case where the table is empty
            if result[0] is None:
                return {
                    "total_transactions": 0,
                    "total_revenue": 0,
                    "total_expenses": 0,
                    "net_income": 0
                }
            
            # Return a dictionary with the results
            return {
                "total_transactions": int(result[0]),
                "total_revenue": float(result[1]) if result[1] is not None else 0,
                "total_expenses": float(result[2]) if result[2] is not None else 0,
                "net_income": float(result[3]) if result[3] is not None else 0
            }
    except psycopg2.Error as e:
        print(f"Error fetching aggregates: {e}")
        return {}

# 30152
