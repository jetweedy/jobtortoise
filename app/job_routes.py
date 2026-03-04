from flask import Blueprint, request, jsonify, render_template, session, redirect
import jetTools
import json
import pprint
pp = pprint.PrettyPrinter(indent=4)

bp = Blueprint("jobs", __name__, url_prefix="/jobs")


@bp.route('/')
def jobs():
    access = jetTools.checkAccess()
    return render_template('jobs/index.html', data={"access":access})














@bp.route('/__setup', strict_slashes=False)
def setup():
    access = jetTools.checkAccess()
    if not access["isAdmin"]:
        return "Admin only."
    jetTools.pgQuery("""
        CREATE TABLE IF NOT EXISTS asset_groups (
            id BIGSERIAL PRIMARY KEY,
            owner_id INT DEFAULT 0,
            label TEXT
        );
    """)
    jetTools.pgQuery("""
        CREATE TABLE IF NOT EXISTS asset_types (
            id BIGSERIAL PRIMARY KEY,
            owner_id INT DEFAULT 0,
            label TEXT,
            fields jsonb
        );
    """)
    jetTools.pgQuery("""
        CREATE TABLE IF NOT EXISTS field_types (
            id BIGSERIAL PRIMARY KEY,
            owner_id INT DEFAULT 0,
            label TEXT,
            inputtype TEXT,
            options jsonb
        );
    """)
    jetTools.pgQuery("""
        CREATE TABLE IF NOT EXISTS jobs (
            id BIGSERIAL PRIMARY KEY,
            owner_id INT DEFAULT 0,
            custom_id TEXT,
            label TEXT,
            asset_group_id INT DEFAULT 0,
            asset_type_id INT DEFAULT 0,
            fields jsonb
        );
    """)
    jetTools.pgQuery("""
        CREATE TABLE IF NOT EXISTS asset_values (
            id BIGSERIAL PRIMARY KEY,
            asset_id TEXT,
            field text,
            value text
        );
    """)
    return "Done."




"""
CREATE TABLE users (
    -- Numeric / IDs
    id BIGSERIAL PRIMARY KEY,        -- Auto-incrementing integer ID
    uuid UUID DEFAULT gen_random_uuid(),  -- Universally unique ID (requires pgcrypto or uuid-ossp)

    -- Strings
    username VARCHAR(50) UNIQUE NOT NULL, -- Length limit if you want a rule
    email TEXT NOT NULL,                  -- Arbitrary length string
    password_hash TEXT NOT NULL,          -- Store password hashes safely

    -- Booleans
    is_active BOOLEAN DEFAULT TRUE,       -- True/False flag

    -- Dates & Times
    created_at TIMESTAMPTZ DEFAULT now(), -- Timestamp with timezone
    last_login TIMESTAMPTZ,               -- Nullable until first login
    birthdate DATE,                       -- Calendar date only

    -- Numbers
    login_count INT DEFAULT 0,            -- 32-bit integer
    account_balance NUMERIC(12,2),        -- Exact decimal, e.g., money

    -- JSON / Semi-structured data
    profile JSONB,                        -- Flexible attributes (bio, prefs, etc.)

    -- Arrays
    roles TEXT[] DEFAULT ARRAY['user'],   -- List of roles

    -- Enumerations
    status user_status NOT NULL DEFAULT 'pending' -- Enum example (see below)
);

"""


