#!/usr/bin/env python

from gsw import SP_from_C, SA_from_SP, CT_from_t, rho, z_from_p


def create_practical_salinity_sensor(reader):

    return


def create_density_sensor(reader):

    return


def calculate_practical_salinity(conductivity, temperature, pressure):
    """Calculates practical salinity given glider conductivity, temperature,
    and pressure using Gibbs gsw SP_from_C function.

    Parameters:
        timestamp, conductivity (S/m), temperature (C), and pressure (bar).

    Returns:
        salinity (psu PSS-78).
    """

    # Convert S/m to mS/cm
    ms_conductivity = conductivity * 10

    return SP_from_C(
        ms_conductivity,
        temperature,
        pressure
    )


def calculate_density(timestamps,
                      temperature, pressure, salinity,
                      latitude, longitude):
    """Calculates density given glider practical salinity, pressure, latitude,
    and longitude using Gibbs gsw SA_from_SP and rho functions.

    Parameters:
        timestamps (UNIX epoch),
        temperature (C), pressure (dbar), salinity (psu PSS-78),
        latitude (decimal degrees), longitude (decimal degrees)

    Returns:
        density (kg/m**3),
    """

    # dBar_pressure = pressure * 10

    absolute_salinity = SA_from_SP(
        salinity,
        pressure,
        longitude,
        latitude
    )

    conservative_temperature = CT_from_t(
        absolute_salinity,
        temperature,
        pressure
    )

    density = rho(
        absolute_salinity,
        conservative_temperature,
        pressure
    )

    return density


def calculate_depth(pressure, latitude):
    """Calculates depth from pressure (dbar) and latitude.  By default, gsw returns depths as negative.  This routine
    returns the absolute values for positive depths.

    Paramters:
        pressure (decibars)
        latitude (decimal degrees)

    Returns:
        depth (meters)
    """

    return abs(z_from_p(pressure, latitude))


