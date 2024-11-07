import psycopg2
from psycopg2 import sql

def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                client_id SERIAL PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(100) UNIQUE
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS phones (
                phone_id SERIAL PRIMARY KEY,
                client_id INTEGER REFERENCES clients(client_id) ON DELETE CASCADE,
                phone VARCHAR(20) UNIQUE
            );
        """)
        conn.commit()

def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clients (first_name, last_name, email)
            VALUES (%s, %s, %s) RETURNING client_id;
        """, (first_name, last_name, email))
        client_id = cur.fetchone()[0]
        if phones:
            for phone in phones:
                add_phone(conn, client_id, phone)
        conn.commit()

def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO phones (client_id, phone)
            VALUES (%s, %s) ON CONFLICT DO NOTHING;
        """, (client_id, phone))
        conn.commit()

def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:
        if first_name:
            cur.execute("UPDATE clients SET first_name = %s WHERE client_id = %s;", (first_name, client_id))
        if last_name:
            cur.execute("UPDATE clients SET last_name = %s WHERE client_id = %s;", (last_name, client_id))
        if email:
            cur.execute("UPDATE clients SET email = %s WHERE client_id = %s;", (email, client_id))
        if phones is not None:
            cur.execute("DELETE FROM phones WHERE client_id = %s;", (client_id,))
            for phone in phones:
                add_phone(conn, client_id, phone)
        conn.commit()

def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM phones WHERE client_id = %s AND phone = %s;", (client_id, phone))
        conn.commit()

def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM clients WHERE client_id = %s;", (client_id,))
        conn.commit()

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        query = """
            SELECT c.client_id, c.first_name, c.last_name, c.email, p.phone
            FROM clients c
            LEFT JOIN phones p ON c.client_id = p.client_id
            WHERE (%s IS NULL OR c.first_name = %s)
            AND (%s IS NULL OR c.last_name = %s)
            AND (%s IS NULL OR c.email = %s)
            AND (%s IS NULL OR p.phone = %s);
        """
        cur.execute(query, (first_name, first_name, last_name, last_name, email, email, phone, phone))
        return cur.fetchall()

with psycopg2.connect(database="netology_db2", user="postgres", password="postgres") as conn:
    create_db(conn)
    
    add_client(conn, "Аркадий", "Иванов", "arkadii.ivanov@mail.ru", ["123456789", "987654321"])
    add_client(conn, "Елена", "Аркадьева", "elena.arkadieva@mail.ru")
    
    add_phone(conn, 1, "555555555")
    
    change_client(conn, 1, email="arkadii.ivanov_updated@mail.ru", phones=["111111111", "222222222"])
    
    delete_phone(conn, 1, "111111111")
    
    delete_client(conn, 2)
    
    print(find_client(conn, first_name="Аркадий"))
