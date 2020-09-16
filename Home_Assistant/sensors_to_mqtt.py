#!/usr/bin/env python3
import argparse
import datetime as dt
import signal
import sys
import threading
import time
from datetime import timedelta

import paho.mqtt.client as mqtt
import yaml
from lib.sensor_functions import *


mqttClient = None
WAIT_TIME_SECONDS = 60
deviceName = None
I2C_bus = None
particle_sensor = PARTICLE_SENSOR_OFF

class ProgramKilled(Exception):
    pass


def signal_handler(signum, frame):
    raise ProgramKilled


class Job(threading.Thread):
    def __init__(self, interval, execute, *args, **kwargs):
        threading.Thread.__init__(self)
        self.daemon = False
        self.stopped = threading.Event()
        self.interval = interval
        self.execute = execute
        self.args = args
        self.kwargs = kwargs

    def stop(self):
        self.stopped.set()
        self.join()

    def run(self):
        while not self.stopped.wait(self.interval.total_seconds()):
            self.execute(*self.args, **self.kwargs)

def on_message(client, userdata, message):
    print (f"Message received: {message.payload.decode()}"  )
    if(message.payload.decode() == "online"):
        send_config_message(client)

def get_air_data(I2C_bus):
    raw_data = I2C_bus.read_i2c_block_data(i2c_7bit_address, AIR_DATA_READ, AIR_DATA_BYTES)
    return extractAirData(raw_data)

def get_air_quality_data(I2C_bus):
    raw_data = I2C_bus.read_i2c_block_data(i2c_7bit_address, AIR_QUALITY_DATA_READ, AIR_QUALITY_DATA_BYTES)
    return extractAirQualityData(raw_data)

def get_light_data(I2C_bus):
    raw_data = I2C_bus.read_i2c_block_data(i2c_7bit_address, LIGHT_DATA_READ, LIGHT_DATA_BYTES)
    return extractLightData(raw_data)

def get_sound_data(I2C_bus):
    raw_data = I2C_bus.read_i2c_block_data(i2c_7bit_address, SOUND_DATA_READ, SOUND_DATA_BYTES)
    return extractSoundData(raw_data)

def get_particle_data(I2C_bus):
    if (particle_sensor != PARTICLE_SENSOR_OFF):
        raw_data = I2C_bus.read_i2c_block_data(i2c_7bit_address, PARTICLE_DATA_READ, PARTICLE_DATA_BYTES)
        return extractParticleData(raw_data, particle_sensor)
    else:
        return None

def updateSensors(channel, I2C_bus):
    air_data = get_air_data(I2C_bus)
    air_quality_data = get_air_quality_data(I2C_bus)
    light_data = get_light_data(I2C_bus)
    sound_data = get_sound_data(I2C_bus)
    particle_data = get_particle_data(I2C_bus)
    payload_str = (
        "{"
        + f'"temperature": {air_data["T_C"]}'
        + f', "pressure": {air_data["P_Pa"]}'
        + f', "humidity": {air_data["H_pc"]}'
        + f', "gas_sensor_resistance": {air_data["G_ohm"]}'
        + f', "illuminance": {light_data["illum_lux"]}'
        + f', "white_light_level": {light_data["white"]}'
        + f', "sound_pressure_level": {sound_data["SPL_dBA"]}'
        + f', "peak_sound_amplitude": {sound_data["peak_amp_mPa"]}'
        + f', "particle_sensor_duty_cycle": {particle_data["duty_cycle_pc"]}'
        + f', "particle_concentration": {particle_data["concentration"]}'
        + f', "air_quality_index": {air_quality_data["AQI"]}'
        + '}'
    )
    mqttClient.publish(
        topic=f"metriful-sensors/{deviceName.lower()}/state",
        payload=payload_str,
        qos=1,
        retain=False,
    )
    payload_str = (
        '{'
        + f'"frequency_band_125_hz_spl": {sound_data["SPL_bands_dB"][0]}'
        + f', "frequency_band_250_hz_spl": {sound_data["SPL_bands_dB"][1]}'
        + f', "frequency_band_500_hz_spl": {sound_data["SPL_bands_dB"][2]}'
        + f', "frequency_band_1000_hz_spl": {sound_data["SPL_bands_dB"][3]}'
        + f', "frequency_band_2000_hz_spl": {sound_data["SPL_bands_dB"][4]}'
        + f', "frequency_band_4000_hz_spl": {sound_data["SPL_bands_dB"][5]}'
        + '}'
    )
    payload_str = payload_str 
    mqttClient.publish(
        topic=f"metriful-sensors/{deviceName.lower()}/sound/attributes",
        payload=payload_str,
        qos=1,
        retain=False,
    )

    payload_str = (
        '{'
        + f'"estimated_co2": {air_quality_data["CO2e"]}'
        + f', "equivalent_breath_voc": {air_quality_data["bVOC"]}'
        + f', "aqi_accuracy": {air_quality_data["AQI_accuracy"]}'
        + '}'
    )
    payload_str = payload_str 
    mqttClient.publish(
        topic=f"metriful-sensors/{deviceName.lower()}/air_quality/attributes",
        payload=payload_str,
        qos=1,
        retain=False,
    )
    payload_str = (
        '{'
        + f'"particle_data_valid": "{particle_data["valid"]}"'
        + '}' 
    )
    mqttClient.publish(
        topic=f"metriful-sensors/{deviceName.lower()}/particles/attributes",
        payload=payload_str,
        qos=1,
        retain=False,
    )


def check_settings(settings):
    if "mqtt" not in settings:
        print("Mqtt not defined in settings.yaml! Please check the documentation")
        sys.stdout.flush()
        sys.exit()
    if "hostname" not in settings["mqtt"]:
        print("Hostname not defined in settings.yaml! Please check the documentation")
        sys.stdout.flush()
        sys.exit()
    if "deviceName" not in settings:
        print("deviceName not defined in settings.yaml! Please check the documentation")
        sys.stdout.flush()
        sys.exit()
    if "client_id" not in settings:
        print("client_id not defined in settings.yaml! Please check the documentation")
        sys.stdout.flush()
        sys.exit()


def send_config_message(mqttClient):
    print("send config message")
    mqttClient.publish(
        topic=f"homeassistant/sensor/metriful_{deviceName.lower()}/temperature/config",
        payload='{"device_class":"temperature",'
                + f"\"name\":\"{deviceName} Temperature\","
                + f"\"state_topic\":\"metriful-sensors/{deviceName.lower()}/state\","
                + '"unit_of_measurement":"°C",'
                + '"value_template":"{{value_json.temperature}}",'
                + f"\"unique_id\":\"metriful_{deviceName.lower()}_temperature\","
                + f"\"availability_topic\":\"metriful-sensors/{deviceName.lower()}/availability\","
                + f"\"device\":{{\"identifiers\":[\"metriful_{deviceName.lower()}\"],"
                + f"\"name\":\"Metriful {deviceName}\",\"model\":\"Metriful\", \"manufacturer\":\"Metriful\"}}"
                + "}",
        qos=1,
        retain=True,
    )
    mqttClient.publish(
        topic=f"homeassistant/sensor/metriful_{deviceName.lower()}/pressure/config",
        payload='{"device_class":"pressure",'
                + f"\"name\":\"{deviceName} Pressure\","
                + f"\"state_topic\":\"metriful-sensors/{deviceName.lower()}/state\","
                + '"unit_of_measurement":"Pa",'
                + '"value_template":"{{value_json.pressure}}",'
                + f"\"unique_id\":\"metriful_{deviceName.lower()}_pressure\","
                + f"\"availability_topic\":\"metriful-sensors/{deviceName.lower()}/availability\","
                + f"\"device\":{{\"identifiers\":[\"metriful_{deviceName.lower()}\"],"
                + f"\"name\":\"Metriful {deviceName}\",\"model\":\"Metriful\", \"manufacturer\":\"Metriful\"}}"
                + "}",
        qos=1,
        retain=True,
    )
    mqttClient.publish(
        topic=f"homeassistant/sensor/metriful_{deviceName.lower()}/humidity/config",
        payload='{"device_class":"humidity",'
                + f"\"name\":\"{deviceName} Humidity\","
                + f"\"state_topic\":\"metriful-sensors/{deviceName.lower()}/state\","
                + '"unit_of_measurement":"%",'
                + '"value_template":"{{value_json.humidity}}",'
                + f"\"unique_id\":\"metriful_{deviceName.lower()}_humidity\","
                + f"\"availability_topic\":\"metriful-sensors/{deviceName.lower()}/availability\","
                + f"\"device\":{{\"identifiers\":[\"metriful_{deviceName.lower()}\"],"
                + f"\"name\":\"Metriful {deviceName}\",\"model\":\"Metriful\", \"manufacturer\":\"Metriful\"}}"
                + "}",
        qos=1,
        retain=True,
    )
    mqttClient.publish(
        topic=f"homeassistant/sensor/metriful_{deviceName.lower()}/gas_sensor_resistance/config",
        payload='{'
                + f"\"name\":\"{deviceName} Gas sensor resistance\","
                + f"\"state_topic\":\"metriful-sensors/{deviceName.lower()}/state\","
                + '"unit_of_measurement":"Ohm",'
                + '"value_template":"{{value_json.gas_sensor_resistance}}",'
                + f"\"unique_id\":\"metriful_{deviceName.lower()}_gas_sensor_resistance\","
                + f"\"availability_topic\":\"metriful-sensors/{deviceName.lower()}/availability\","
                + f"\"device\":{{\"identifiers\":[\"metriful_{deviceName.lower()}\"],"
                + f"\"name\":\"Metriful {deviceName}\",\"model\":\"Metriful\", \"manufacturer\":\"Metriful\"}}"
                + "}",
        qos=1,
        retain=True,
    )
    mqttClient.publish(
        topic=f"homeassistant/sensor/metriful_{deviceName.lower()}/illuminance/config",
        payload='{"device_class":"illuminance",'
                + f"\"name\":\"{deviceName} Illuminance\","
                + f"\"state_topic\":\"metriful-sensors/{deviceName.lower()}/state\","
                + '"unit_of_measurement":"lux",'
                + '"value_template":"{{value_json.illuminance}}",'
                + f"\"unique_id\":\"metriful_{deviceName.lower()}_illuminance\","
                + f"\"availability_topic\":\"metriful-sensors/{deviceName.lower()}/availability\","
                + f"\"device\":{{\"identifiers\":[\"metriful_{deviceName.lower()}\"],"
                + f"\"name\":\"Metriful {deviceName}\",\"model\":\"Metriful\", \"manufacturer\":\"Metriful\"}}"
                + "}",
        qos=1,
        retain=True,
    )
    mqttClient.publish(
        topic=f"homeassistant/sensor/metriful_{deviceName.lower()}/white_light_level/config",
        payload='{'
                + f"\"name\":\"{deviceName} White Light Level\","
                + f"\"state_topic\":\"metriful-sensors/{deviceName.lower()}/state\","
                # + '"unit_of_measurement":"",'
                + '"value_template":"{{value_json.white_light_level}}",'
                + f"\"unique_id\":\"metriful_{deviceName.lower()}_white_light_level\","
                + f"\"availability_topic\":\"metriful-sensors/{deviceName.lower()}/availability\","
                + f"\"device\":{{\"identifiers\":[\"metriful_{deviceName.lower()}\"],"
                + f"\"name\":\"Metriful {deviceName}\",\"model\":\"Metriful\", \"manufacturer\":\"Metriful\"}}"
                "}",
        qos=1,
        retain=True,
    )
    mqttClient.publish(
        topic=f"homeassistant/sensor/metriful_{deviceName.lower()}/sound_pressure_level/config",
        payload='{'
                + f"\"name\":\"{deviceName} A-weighted Sound Pressure Level\","
                + f"\"state_topic\":\"metriful-sensors/{deviceName.lower()}/state\","
                + '"unit_of_measurement":"dBA",'
                + '"value_template":"{{value_json.sound_pressure_level}}",'
                + f"\"unique_id\":\"metriful_{deviceName.lower()}_sound_pressure_level\","
                + f"\"json_attributes_topic\":\"metriful-sensors/{deviceName.lower()}/sound/attributes\","
                + f"\"availability_topic\":\"metriful-sensors/{deviceName.lower()}/availability\","
                + f"\"device\":{{\"identifiers\":[\"metriful_{deviceName.lower()}\"],"
                + f"\"name\":\"Metriful {deviceName}\",\"model\":\"Metriful\", \"manufacturer\":\"Metriful\"}}"
                + "}",
        qos=1,
        retain=True,
    )
    mqttClient.publish(
        topic=f"homeassistant/sensor/metriful_{deviceName.lower()}/peak_sound_amplitude/config",
        payload='{'
                + f"\"name\":\"{deviceName} Peak Sound Amplitude\","
                + f"\"state_topic\":\"metriful-sensors/{deviceName.lower()}/state\","
                + '"unit_of_measurement":"mPa",'
                + '"value_template":"{{value_json.peak_sound_amplitude}}",'
                + f"\"unique_id\":\"metriful_{deviceName.lower()}_peak_sound_amplitude\","
                + f"\"availability_topic\":\"metriful-sensors/{deviceName.lower()}/availability\","
                + f"\"device\":{{\"identifiers\":[\"metriful_{deviceName.lower()}\"],"
                + f"\"name\":\"Metriful {deviceName}\",\"model\":\"Metriful\", \"manufacturer\":\"Metriful\"}}"
                + "}",
        qos=1,
        retain=True,
    )
    mqttClient.publish(
        topic=f"homeassistant/sensor/metriful_{deviceName.lower()}/particle_sensor_duty_cycle/config",
        payload='{'
                + f"\"name\":\"{deviceName} Particle Sensor Duty Cycle\","
                + f"\"state_topic\":\"metriful-sensors/{deviceName.lower()}/state\","
                + '"unit_of_measurement":"%",'
                + '"value_template":"{{value_json.particle_sensor_duty_cycle}}",'
                + f"\"unique_id\":\"metriful_{deviceName.lower()}_particle_sensor_duty_cycle\","
                + f"\"json_attributes_topic\":\"metriful-sensors/{deviceName.lower()}/particles/attributes\","
                + f"\"availability_topic\":\"metriful-sensors/{deviceName.lower()}/availability\","
                + f"\"device\":{{\"identifiers\":[\"metriful_{deviceName.lower()}\"],"
                + f"\"name\":\"Metriful {deviceName}\",\"model\":\"Metriful\", \"manufacturer\":\"Metriful\"}}"
                + "}",
        qos=1,
        retain=True,
    )
    mqttClient.publish(
        topic=f"homeassistant/sensor/metriful_{deviceName.lower()}/particle_concentration/config",
        payload='{'
                + f"\"name\":\"{deviceName} Particle Concentration\","
                + f"\"state_topic\":\"metriful-sensors/{deviceName.lower()}/state\","
                + '"unit_of_measurement":"ppL",'
                + '"value_template":"{{value_json.particle_concentration}}",'
                + f"\"unique_id\":\"metriful_{deviceName.lower()}_particle_concentration\","
                + f"\"json_attributes_topic\":\"metriful-sensors/{deviceName.lower()}/particles/attributes\","
                + f"\"availability_topic\":\"metriful-sensors/{deviceName.lower()}/availability\","
                + f"\"device\":{{\"identifiers\":[\"metriful_{deviceName.lower()}\"],"
                + f"\"name\":\"Metriful {deviceName}\",\"model\":\"Metriful\", \"manufacturer\":\"Metriful\"}}"
                + "}",
        qos=1,
        retain=True,
    )
    mqttClient.publish(
        topic=f"homeassistant/sensor/metriful_{deviceName.lower()}/air_quality_index/config",
        payload='{'
                + f"\"name\":\"{deviceName} Air Quality Index\","
                + f"\"state_topic\":\"metriful-sensors/{deviceName.lower()}/state\","
                + '"unit_of_measurement":"ppm",'
                + '"value_template":"{{value_json.air_quality_index}}",'
                + f"\"unique_id\":\"metriful_{deviceName.lower()}air_quality_index\","
                + f"\"json_attributes_topic\":\"metriful-sensors/{deviceName.lower()}/air_quality/attributes\","
                + f"\"availability_topic\":\"metriful-sensors/{deviceName.lower()}/availability\","
                + f"\"device\":{{\"identifiers\":[\"metriful_{deviceName.lower()}\"],"
                + f"\"name\":\"Metriful {deviceName}\",\"model\":\"Metriful\", \"manufacturer\":\"Metriful\"}}"
                + "}",
        qos=1,
        retain=True,
    )
    

    mqttClient.publish(f"metriful-sensors/{deviceName.lower()}/availability", "online", retain=True)


def _parser():
    """Generate argument parser"""
    parser = argparse.ArgumentParser()
    parser.add_argument("settings", help="path to the settings file")
    return parser


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        client.subscribe("hass/status")
    else:
        print("Connection failed")

if __name__ == "__main__":
    args = _parser().parse_args()
    with open(args.settings) as f:
        # use safe_load instead load
        settings = yaml.safe_load(f)
    check_settings(settings)
    mqttClient = mqtt.Client(client_id=settings["client_id"])
    mqttClient.on_connect = on_connect                      #attach function to callback
    mqttClient.on_message = on_message
    deviceName = settings["deviceName"]
    mqttClient.will_set(f"metriful-sensors/{deviceName.lower()}/availability", "offline", retain=True)
    if "user" in settings["mqtt"]:
        mqttClient.username_pw_set(
            settings["mqtt"]["user"], settings["mqtt"]["password"]
        )  # Username and pass if configured otherwise you should comment out this
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    if "port" in settings["mqtt"]:
        mqttClient.connect(settings["mqtt"]["hostname"], settings["mqtt"]["port"])
    else:
        mqttClient.connect(settings["mqtt"]["hostname"], 1883)
    try:
        send_config_message(mqttClient)
    except:
        print("something whent wrong")
    # setup_ms430(settings)
    (GPIO, I2C_bus) = SensorHardwareSetup()
    if settings['particleSensor'] == 'PARTICLE_SENSOR_SDS011':
        particle_sensor = PARTICLE_SENSOR_SDS011
    elif settings['particleSensor'] == 'PARTICLE_SENSOR_PPD42':
        particle_sensor = PARTICLE_SENSOR_PPD42
    else:
        particle_sensor = PARTICLE_SENSOR_OFF
    
    # Apply the chosen settings
    if (particle_sensor != PARTICLE_SENSOR_OFF):
        I2C_bus.write_i2c_block_data(i2c_7bit_address, PARTICLE_SENSOR_SELECT_REG, [particle_sensor])
    I2C_bus.write_i2c_block_data(i2c_7bit_address, CYCLE_TIME_PERIOD_REG, [settings['cycle_period']])
    #########################################################
    print("Entering cycle mode and waiting for data. Press ctrl-c to exit.")
    I2C_bus.write_byte(i2c_7bit_address, CYCLE_MODE_CMD)
    cb = lambda channel, arg1=I2C_bus: updateSensors(channel, arg1)
    GPIO.add_event_callback(READY_pin, cb)

    mqttClient.loop_start()

    while True:
        try:
            sys.stdout.flush()
            time.sleep(1)
        except ProgramKilled:
            print("Program killed: running cleanup code")
            mqttClient.publish(f"metriful-sensors/{deviceName.lower()}/availability", "offline", retain=True)
            mqttClient.disconnect()
            mqttClient.loop_stop()
            sys.stdout.flush()
            break