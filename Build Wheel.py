# Databricks notebook source
# DBTITLE 1,Build wheel fix
import subprocess
import shutil
import os

# Ensure 'build' package is installed
subprocess.run("python -m pip install --upgrade build", shell=True)

def create_wheel(path):
    subprocess.run("python -m build --wheel", cwd=path, shell=True)  # Only create .whl, not .tar.gz

# def delete_egg_info(path):
#     egg_info_folder = next((f for f in os.listdir(path) if f.endswith('.egg-info')), None)
#     if egg_info_folder:
#         shutil.rmtree(os.path.join(path, egg_info_folder))

# def delete_build_folder(path):
#     build_folder = os.path.join(path, "build")
#     if os.path.exists(build_folder):
#         shutil.rmtree(build_folder)

package_path = "/Workspace/Users/manojkumarbahuguna111@gmail.com/wind-turbines-project/"  # update with your package path

create_wheel(package_path)
# delete_egg_info(package_path)
# delete_build_folder(package_path)

# COMMAND ----------

dist_folder = os.path.join(package_path, "dist")
volume_path = "/Volumes/demo/raw/files/wheel/"  # update with your volume path

os.makedirs(volume_path, exist_ok=True)

wheel_file = next((f for f in os.listdir(dist_folder) if f.endswith('.whl')), None)
if wheel_file:
    shutil.copy2(os.path.join(dist_folder, wheel_file), os.path.join(volume_path, wheel_file))

# COMMAND ----------

pip install "/Volumes/demo/raw/files/wheel/wind_turbines_project-0.1.2-py3-none-any.whl"
