import logging
from gncutils.constants import LLAT_SENSORS, NGDAC_VAR_NAMES

logger = logging.getLogger(__file__)




def validate_llat_sensors(sensor_configs):

    return validate_sensors(sensor_configs, LLAT_SENSORS)


def validate_sensors(sensor_configs, required_sensor_names):
    validated = True

    sensor_names = sensor_configs.keys()

    for required_sensor in required_sensor_names:
        if required_sensor not in sensor_names:
            logger.warning('Missing llat sensor {:s}'.format(required_sensor))
            validate = False

    return validated


def validate_ngdac_var_names(sensor_configs):

    validated = True

    nc_var_names = [sensor_configs[s]['nc_var_name'] for s in sensor_configs]

    for ngdac_var in NGDAC_VAR_NAMES:
        if ngdac_var not in nc_var_names:
            logger.warning('Missing required IOOS NGDAC nc_var_name {:s}'.format(ngdac_var))
            validated = False

    return validated