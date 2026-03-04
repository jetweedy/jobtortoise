from flask import Flask, request, jsonify, render_template, session, redirect
import pymysql
import os, sys
import json
import requests
from bs4 import BeautifulSoup
import feedparser
import urllib.request
from werkzeug.security import generate_password_hash, check_password_hash
import logging
import time
from datetime import timedelta

from dotenv import load_dotenv
load_dotenv()
import configparser
cfg = configparser.ConfigParser()
cfg.read(os.path.join(os.getcwd(), '.env'))

import pprint
pp = pprint.PrettyPrinter(indent=4)

import jetTools

## Import blueprints / other routes
from job_routes import bp as jobs_bp



#jetTools.initSQLite()
jetTools.initPostGresApp();

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, "templates"),
    static_folder=os.path.join(BASE_DIR, "static"),
    static_url_path="/static"
)


app.secret_key = "G3nPy4pp!"
app.permanent_session_lifetime = timedelta(days=7)

app.config['TEMPLATES_AUTO_RELOAD'] = True




# Register your blueprint(s) from the imports above
app.register_blueprint(jobs_bp)



@app.route('/')
def home():
    access = jetTools.checkAccess()
    return render_template("index.html", data={"access":access})


@app.route('/testLocalPostGresSQL')
@app.route('/testLocalPostGresSQL/')
def dbTest():
    return ""
    access = jetTools.checkAccess()
    ### create a table
    print(jetTools.dbExecute(False, """
    CREATE TABLE IF NOT EXISTS testTable (
      id BIGSERIAL PRIMARY KEY,
      rantext TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """, return_rows=False))
    ### UNCOMMENT BELOW TO insert a row
    #print(jetTools.dbExecute(False,
    #    "INSERT INTO testTable (rantext) VALUES (:rantext) RETURNING id",
    #    params={"rantext":"54321"},
    #    return_rows=True))
    ### select into dict x
    x = jetTools.dbExecute(False
        , "SELECT id, rantext FROM testTable ORDER BY id DESC;")
    #jetTools.dbExecute(False, "DROP TABLE testTable;")
    ### Return jsonify(x) as output to browser
    return jsonify(x)




@app.route('/register', methods=['GET', 'POST'])
@app.route('/register/', methods=['GET', 'POST'])
def register():
    access = jetTools.checkAccess()
    user_email = session.get('user_email')
    if not access["isAdmin"]:
        #return "You must be the Admin to register a user."
        #return redirect('/login')
        return "Unauthorized", 403
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']
        hash_pw = generate_password_hash(password)
        # Check if email already exists
        check = jetTools.sqliteQuery("SELECT * FROM users WHERE email = ?", (email,))
        if not check["success"]:
            return f"Database error: {check['error']}", 500
        if check["data"]:
            return "Email already registered.", 400
        # Insert user
        insert = jetTools.sqliteQuery(
            "INSERT INTO users (email, password_hash) VALUES (?, ?)",
            (email, hash_pw)
        )
        if not insert["success"]:
            return f"Insert error: {insert['error']}", 500
        session['user_email'] = email
        return redirect('/')
    return render_template('register.html', data={"access":access})



@app.route('/login', methods=['GET', 'POST'])
@app.route('/login/', methods=['GET', 'POST'])
def login():
    access = jetTools.checkAccess()
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']
        result = jetTools.pgQuery("SELECT * FROM users WHERE email = %s", (email,))
        if len(result)!=1:
            return "No user found with that email.", 400
        user = result[0]
        pp.pprint(user)
        if check_password_hash(user["password"], password):
            session.permanent = True
            session['user_email'] = user["email"]
            session['user_id'] = user["id"]
            return redirect('/')
        else:
            return "Incorrect password.", 400
    return render_template('login.html', data={"access":access})



@app.route('/logout')
@app.route('/logout/')
def logout():
    session.pop('user_email', None)
    session.pop('user_id', None)
    return redirect('/')



@app.route("/admin/users")
@app.route("/admin/users/")
def admin_users():
    access = jetTools.checkAccess()
    if not access["isAdmin"]:
        return "Unauthorized", 403
    users = jetTools.pgQuery("SELECT email FROM users")
    return render_template("manage-users.html", data={"access":access, "users":users})


@app.route("/admin/add-user", methods=["POST"])
@app.route("/admin/add-user/", methods=["POST"])
def admin_add_user():
    access = jetTools.checkAccess()
    if not access["isAdmin"]:
        return jsonify({"message": "Unauthorized"}), 403
    data = request.get_json()
    email = data.get("email")
    password = data.get("password")
    if not email or not password:
        return jsonify({"message": "Missing fields"}), 400
    password_hash = generate_password_hash(password)
    result = jetTools.pgQuery(
        "INSERT INTO users (email, password) VALUES (%s, %s)",
        params=(email, password_hash)
    )
    if result["rowcount"]:
        return jsonify({"message": "User added successfully."})
    else:
        return jsonify({"message": f"Error: {result['error']}"}), 500


@app.route("/admin/edit-user", methods=["POST"])
@app.route("/admin/edit-user/", methods=["POST"])
def admin_edit_user():
    access = jetTools.checkAccess()
    if not access["isAdmin"]:
        return jsonify({"message": "Unauthorized"}), 403
    data = request.get_json()
    email = data.get("email")
    password = data.get("password")
    if not email or not password:
        return jsonify({"message": "Missing fields"}), 400
    password_hash = generate_password_hash(password)
    result = jetTools.pgQuery(
        "UPDATE users SET password = %s WHERE email = %s",
        params=(password_hash, email)
    )
    if result["rowcount"]:
        return jsonify({"message": "Password updated."})
    else:
        return jsonify({"message": f"Error: {result['error']}"}), 500

@app.route("/admin/delete-user", methods=["POST"])
@app.route("/admin/delete-user/", methods=["POST"])
def admin_delete_user():
    access = jetTools.checkAccess()
    if not access["isAdmin"]:
        return jsonify({"message": "Unauthorized"}), 403
    data = request.get_json()
    email = data.get("email")
    result = jetTools.pgQuery(
        "DELETE FROM users WHERE email = %s",
        params=(email,)
    )
    if result["rowcount"]:
        return jsonify({"message": "User deleted."})
    else:
        return jsonify({"message": f"Error: {result['error']}"}), 500










if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)





