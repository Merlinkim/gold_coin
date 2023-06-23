# my_package/my_plugin.py
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

# Creating a flask blueprint to integrate the templates and static folder
bp = Blueprint(
    "test_plugin",
    __name__,
    template_folder="templates",  # registers airflow/plugins/templates as a Jinja template folder
    static_folder="static",
    static_url_path="/Users/inseongkim/code/coin/coin/my_package",
)


class MyAirflowPlugin(AirflowPlugin):
    name = "my_namespace"
    flask_blueprints = [bp]