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




@bp.route('/ipeds_schools')
def get_ipeds_schools():
    sql = "SELECT * FROM ipeds_schools;"
    q = jetTools.pgQuery(sql)
    return jsonify(q)


@bp.route('/update_hr_system', methods=['GET', 'POST'])
@bp.route('/update_hr_system/', methods=['GET', 'POST'])
def update_hr_system():
    r = {};
    unitid = str(request.form['unitid'])
    hr_system = request.form['hr_system']
    r = jetTools.pgQuery("UPDATE ipeds_schools SET hr_system = %s WHERE unitid = %s", (hr_system, unitid));
    return jsonify(r);




@bp.route('/setup', strict_slashes=False)
def setup():
    access = jetTools.checkAccess()
    if not access["isAdmin"]:
        return "Admin only."
    jetTools.pgQuery("""
        CREATE TABLE IF NOT EXISTS peopleadmin_postings (
            id BIGSERIAL PRIMARY KEY,

            -- identity / provenance
            school_input TEXT NOT NULL,
            base_url TEXT NOT NULL,
            query TEXT NOT NULL DEFAULT '',

            posting_id TEXT,
            url TEXT NOT NULL,

            -- summary fields
            title TEXT NOT NULL,
            location TEXT,
            department TEXT,

            -- detail fields (raw)
            salary TEXT,
            salary_min TEXT,
            salary_max TEXT,

            -- detail fields (numeric for analytics)
            salary_min_num NUMERIC(12,2),
            salary_max_num NUMERIC(12,2),

            posted_date DATE,
            close_date DATE,
            open_until_filled BOOLEAN,

            employment_type TEXT,
            time_limit TEXT,
            full_time_or_part_time TEXT,
            special_instructions TEXT,

            -- full page capture
            detail_text TEXT,
            detail_html TEXT,

            -- metadata
            scraped_at TIMESTAMPTZ NOT NULL DEFAULT now(),

            CONSTRAINT uq_peopleadmin_posting UNIQUE (base_url, posting_id)
        );
    """)
    jetTools.pgQuery("""
        CREATE INDEX IF NOT EXISTS idx_peopleadmin_school_input
            ON peopleadmin_postings (school_input);
    """)
    jetTools.pgQuery("""
        CREATE INDEX IF NOT EXISTS idx_peopleadmin_posted_date
            ON peopleadmin_postings (posted_date);
    """)
    jetTools.pgQuery("""
        CREATE INDEX IF NOT EXISTS idx_peopleadmin_close_date
            ON peopleadmin_postings (close_date);
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


