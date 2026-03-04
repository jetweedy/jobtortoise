from flask import Flask, request, jsonify, render_template, session, redirect
import pymysql
import os, sys
import json
import sqlite3
from bs4 import BeautifulSoup
import pandas as pd
import requests
from pathlib import Path

import psycopg2
from psycopg2 import OperationalError

from dotenv import load_dotenv
load_dotenv()
import configparser
cfg = configparser.ConfigParser()
cfg.read(os.path.join(os.getcwd(), '.env'))

from werkzeug.security import generate_password_hash, check_password_hash

import pprint
pp = pprint.PrettyPrinter(indent=4)


ADMINS = [cfg["settings"]["admin_email"]]


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#print("BASE_DIR:", BASE_DIR)
SQLITE_DB_PATH = os.path.join(BASE_DIR, "sqlite/app.db")
#print("SQLITE_DB_PATH:", SQLITE_DB_PATH)






def checkAccess(x=None):
    r = {"isAdmin":False, "isLoggedIn":False}
    if ('user_email' in session):
        r["isLoggedIn"] = True
        if (session['user_email'] in ADMINS):
            r["isAdmin"] = True
    if x:
        r["x"] = x
    return r


















def initPostGresApp():
    pgQuery("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL
        );
    """)

    rows = pgQuery("SELECT email, password FROM users WHERE email = %s", (cfg["settings"]["admin_email"],))
    if (not rows) or (len(rows)<0):
        pgQuery("INSERT INTO users (email, password) VALUES (%s, %s)", (cfg["settings"]["admin_email"], generate_password_hash(cfg["settings"]["admin_password"])))






def pgQuery(query, params=None):
    """
    Run a query safely against PostgreSQL.
    - SELECT: returns list of dicts (column names as keys).
    - INSERT/UPDATE/DELETE: returns {"rowcount": n, "insert_id": x if available}.
    - Supports RETURNING clauses.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=cfg["postgres"]["db"],
            user=cfg["postgres"]["user"],
            password=cfg["postgres"]["password"],
            host=cfg["postgres"]["host"],
            port=cfg["postgres"]["port"]
        )
        with conn, conn.cursor() as cur:
            cur.execute(query, params)

            # Detect query type
            qtype = query.strip().split()[0].lower()

            if qtype == "select":
                col_names = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                return [dict(zip(col_names, row)) for row in rows]

            elif "returning" in query.lower():
                # e.g. INSERT ... RETURNING id
                col_names = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                conn.commit()
                return {
                    "rowcount": cur.rowcount,
                    "returning": [dict(zip(col_names, row)) for row in rows]
                }

            else:
                conn.commit()
                return {"rowcount": cur.rowcount}

    except OperationalError as e:
        print("Database connection failed:", e)
    except Exception as e:
        print("Error running query:", e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()










def sqliteQuery(query, params=()):
    result = {"success": False, "data": None, "error": None}
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        conn.row_factory = sqlite3.Row  # allows dict-like row access
        cur = conn.cursor()
        cur.execute(query, params)
        query_type = query.strip().split()[0].upper()
        if query_type == "SELECT":
            rows = cur.fetchall()
            result["data"] = [dict(row) for row in rows]
        else:
            conn.commit()
            result["data"] = {
                "rowcount": cur.rowcount,
                "lastrowid": cur.lastrowid if query_type == "INSERT" else None
            }
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
    finally:
        conn.close()
    return result


def initSQLite():
    db_path = SQLITE_DB_PATH
    print("db_path:", db_path)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    # Create / migrate schema
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL
            );
        """)
        conn.commit()
        result = sqliteQuery(
            "INSERT INTO users (email, password_hash) VALUES (?, ?)",
            params=(cfg["settings"]["admin_email"], generate_password_hash(cfg["settings"]["admin_password"]))
        )
    finally:
        conn.close()



