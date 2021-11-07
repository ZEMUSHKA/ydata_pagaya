#!/usr/bin/env bash
# Determine if we are running on the master node.
# 0 - running on master
# 1 - running on a task or core node
check_if_master() {
    python - <<'__SCRIPT__'
import sys
import json

instance_file = "/mnt/var/lib/info/instance.json"
is_master = False
try:
    with open(instance_file) as f:
        props = json.load(f)

    is_master = props.get('isMaster', False)
except IOError as ex:
    pass # file will not exist when testing on a non-emr machine

if is_master:
    sys.exit(0)
else:
    sys.exit(1)
__SCRIPT__
}

sudo yum -y install python36-pip htop tmux git iotop libjpeg-devel

if check_if_master
then
  sudo pip-3.6 install jupyter findspark kaggle nbdime
  /usr/local/bin/jupyter notebook --generate-config
  echo "c.NotebookApp.ip = '*'" >> /home/hadoop/.jupyter/jupyter_notebook_config.py
  echo "c.NotebookApp.port = 8888" >> /home/hadoop/.jupyter/jupyter_notebook_config.py
  echo "c.NotebookApp.token = ''" >> /home/hadoop/.jupyter/jupyter_notebook_config.py
  sudo /usr/local/bin/jupyter serverextension enable --py nbdime --system
  sudo /usr/local/bin/jupyter nbextension install --py nbdime --system
  sudo /usr/local/bin/jupyter nbextension enable --py nbdime --system
  echo "jupyter notebook --no-browser" > /home/hadoop/start_jupyter.sh
  sudo chmod +x /home/hadoop/start_jupyter.sh
  # git clone https://github.com/ZEMUSHKA/ydata_lsml.git /home/hadoop/ydata_lsml
  # sudo cp /home/hadoop/ydata_lsml/libboost_program_options.so.1.73.0 /usr/lib64/
fi

sudo pip-3.6 install sklearn numpy scipy pandas matplotlib ipywidgets tqdm

# vw
# wget http://finance.yendor.com/ML/VW/Binaries/vw-8.20190624 -O /home/hadoop/vw
# sudo chmod +x /home/hadoop/vw
