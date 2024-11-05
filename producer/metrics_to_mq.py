import sys
import os
import subprocess
import configparser

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore

try:
    import kombu
    import kombu.exceptions
    from kombu import pools
except ImportError:
    kombu = None

DEFAULT_APP_YAML = "app.yml"
CONFIG_PREFIX = "PULSAR_CONFIG_"
PULSAR_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if "PULSAR_CONFIG_DIR" in os.environ:
    PULSAR_CONFIG_DIR = os.path.abspath(os.environ["PULSAR_CONFIG_DIR"])
else:
    PULSAR_CONFIG_DIR = PULSAR_ROOT_DIR

def _find_default_app_config(*config_dirs):
    for config_dir in config_dirs:
        app_config_path = os.path.join(config_dir, DEFAULT_APP_YAML)
        if os.path.exists(app_config_path):
            return app_config_path
    return None

def absolute_config_path(path, config_dir):
    if path and not os.path.isabs(path):
        path = os.path.join(config_dir, path)
    return path

def apply_env_overrides_and_defaults(conf):
    override_prefix = "%sOVERRIDE_" % CONFIG_PREFIX
    for key in os.environ:
        if key == 'PULSAR_CONFIG_DIR':
            conf['config_dir'] = os.environ[key]
        elif key.startswith(override_prefix):
            config_key = key[len(override_prefix):].lower()
            conf[config_key] = os.environ[key]
        elif key.startswith(CONFIG_PREFIX):
            config_key = key[len(CONFIG_PREFIX):].lower()
            if config_key not in conf:
                conf[config_key] = os.environ[key]
    return conf

def load_app_configuration(app_conf_path=None, local_conf=None, config_dir=PULSAR_CONFIG_DIR):
    """
    """
    local_conf = local_conf or {}
    local_conf['config_dir'] = config_dir
    if app_conf_path is None and "app_config" in local_conf:
        app_conf_path = absolute_config_path(local_conf["app_config"], config_dir)
        if not os.path.exists(app_conf_path) and os.path.exists(app_conf_path + ".sample"):
            app_conf_path = app_conf_path + ".sample"

    if app_conf_path:
        if yaml is None:
            raise Exception("Cannot load configuration from file %s, pyyaml is not available." % app_conf_path)

        with open(app_conf_path) as f:
            app_conf = yaml.safe_load(f) or {}
            local_conf.update(app_conf)

    return apply_env_overrides_and_defaults(local_conf)


def main():

    conf = load_app_configuration(app_conf_path="/opt/pulsar/config/app.yml")
    print(conf)

    subprocess.run(["sh","./cluster_util-condor.sh"]) 

if __name__ == '__main__':
    main()

