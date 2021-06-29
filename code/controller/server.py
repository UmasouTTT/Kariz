#!/usr/bin/python3
"""
Main module of the server file
"""

# 3rd party moudles
from flask import render_template
import connexion
import api
import config as cfg



# Create the application instance
app = connexion.App(__name__, specification_dir="./")

# Read the swagger.yml file to configure the endpoints
app.add_api("controller.yml")

# create a URL route in our application for "/"
@app.route("/")
def home():
	return render_template("home.html")


if __name__ == "__main__":
    collector = api.start_estimator()
    controller = api.start_controller()
    app.run(debug=True, port=cfg.controller_port,host=cfg.controller_host)
