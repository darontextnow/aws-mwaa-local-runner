"""Plugin which adds the DBT menu and DBT Data Catalog submenu to main menus in Airflow UI.

All the commented out imports and code was the previous version of the DBT Catalog which was hosted in S3.
This previous version was problematic as everyone had to login to AWS fully/separately to get to updated Catalog.

New version is simply an external link to the de-airflow repo web pages hosted as part of GH pages functionality.

"""
from __future__ import annotations
# from flask import Blueprint
# from flask_appbuilder import BaseView, expose
# from airflow.configuration import conf
from airflow.plugins_manager import AirflowPlugin
# from airflow.security import permissions
# from airflow.www.auth import has_access

# dags_dir = conf.get("core", "dags_folder")
# root_dir = f"{dags_dir}/dag_dependencies/custom_views/dbt_data_catalog"


# class DBTDataCatalogView(BaseView):
#    """Creating a Flask-AppBuilder View"""
#
#    default_view = "index"
#
#    @expose("/")
#    @has_access(
#        [
#            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
#        ]
#    )
#    def index(self):
#        """Create default view"""
#        return self.render_template("index.html", name="DBT Data Catalog")


# Creating a flask blueprint
# bp = Blueprint(
#    "dbt_plugin",
#    __name__,
#    template_folder=root_dir,
#    static_folder=root_dir,
#    static_url_path="/dbtdatacatalogview",
# )

# Add external link to Airflow Main Menus
dbt_catalog_link = {
    "name": "DBT Data Catalog",
    "href": "https://tn-data-catalog.prod.textnow.io/",  # link to current DBT web host in s3 bucket
    "category": "DBT"
}


class DbtPlugin(AirflowPlugin):
    """Defining the plugin class"""

    name = "DBT Plugin"
    # flask_blueprints = [bp]
    # appbuilder_views = [{"name": "Data Catalog", "category": "DBT", "view": DBTDataCatalogView()}]
    appbuilder_menu_items = [dbt_catalog_link]
