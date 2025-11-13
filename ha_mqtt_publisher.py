#!/usr/bin/env python3
"""
Home Assistant MQTT Publisher for Anker Solix devices.

This script collects data from Anker Solix devices using the API and publishes
it to an MQTT broker for Home Assistant integration. It includes MQTT discovery
support for automatic device and entity creation in Home Assistant.

Usage:
    python ha_mqtt_publisher.py [options]

Example:
    python ha_mqtt_publisher.py --live --mqtt --rt --interval 10 --no-ev
"""

import argparse
import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt_client
from aiohttp import ClientSession

from api.api import AnkerSolixApi
from api.apitypes import SolixDeviceType
from api.mqtt_factory import create_mqtt_device

# Try to load python-dotenv for .env file support
try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_credentials(env_file: Optional[str] = None) -> Dict[str, Optional[str]]:
    """Load credentials from environment variables or .env file."""

    # Load .env file if specified or if default exists
    if env_file or Path(".env").exists():
        if DOTENV_AVAILABLE:
            env_path = env_file or ".env"
            logger.info(f"Loading credentials from {env_path}")
            load_dotenv(env_path)
        else:
            logger.warning("python-dotenv not installed. Install with: pip install python-dotenv")
            if env_file:
                logger.error(f"Cannot load specified .env file: {env_file}")
                return {}

    # Get credentials from environment
    credentials = {
        'email': os.getenv('ANKERUSER') or os.getenv('ANKER_USER') or os.getenv('ANKER_EMAIL'),
        'password': os.getenv('ANKERPASSWORD') or os.getenv('ANKER_PASSWORD'),
        'country': os.getenv('ANKERCOUNTRY') or os.getenv('ANKER_COUNTRY'),
        'mqtt_host': os.getenv('MQTT_HOST'),
        'mqtt_port': os.getenv('MQTT_PORT'),
        'mqtt_user': os.getenv('MQTT_USER'),
        'mqtt_password': os.getenv('MQTT_PASSWORD'),
    }

    # Log which credentials were found (without values for security)
    found_creds = [k for k, v in credentials.items() if v is not None]
    if found_creds:
        logger.info(f"Found credentials for: {', '.join(found_creds)}")

    return credentials

def validate_required_credentials(credentials: Dict[str, Optional[str]]) -> bool:
    """Validate that required Anker credentials are available."""
    required = ['email', 'password', 'country']
    missing = [k for k in required if not credentials.get(k)]

    if missing:
        logger.error(f"Missing required credentials: {', '.join(missing)}")
        logger.error("Please set environment variables or create a .env file with:")
        logger.error("  ANKERUSER=your_email@example.com")
        logger.error("  ANKERPASSWORD=your_password")
        logger.error("  ANKERCOUNTRY=DE  # Your country code")
        logger.error("Optional MQTT settings:")
        logger.error("  MQTT_HOST=localhost")
        logger.error("  MQTT_PORT=1883")
        logger.error("  MQTT_USER=mqtt_username")
        logger.error("  MQTT_PASSWORD=mqtt_password")
        return False

    return True

class HomeAssistantMqttPublisher:
    """Publisher for Home Assistant MQTT integration."""

    def __init__(
        self,
        mqtt_host: str = "localhost",
        mqtt_port: int = 1883,
        mqtt_user: Optional[str] = None,
        mqtt_password: Optional[str] = None,
        ha_discovery_prefix: str = "homeassistant",
        device_prefix: str = "anker_solix_mqtt"
    ):
        """Initialize the MQTT publisher."""
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.mqtt_user = mqtt_user
        self.mqtt_password = mqtt_password
        self.ha_discovery_prefix = ha_discovery_prefix
        self.device_prefix = device_prefix
        self.mqtt_client: Optional[mqtt_client.Client] = None
        self.api: Optional[AnkerSolixApi] = None
        self.published_discoveries = set()
        self.device_entities = {}  # Track which entities exist for each device
        self._pending_commands = []
        self._command_event = None  # Will be created when async loop starts
        self._keepalive_enabled = True  # Track keep-alive state
        self._last_keepalive = 0  # Track last keep-alive trigger
        self._command_queue_timestamp = 0  # Track when commands were last added
        self._max_command_wait_time = 60  # Maximum time to wait for command processing (seconds)

    def connect_mqtt(self) -> bool:
        """Connect to MQTT broker."""
        try:
            # Use modern VERSION2 callback API
            self.mqtt_client = mqtt_client.Client(callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2)
            if self.mqtt_user and self.mqtt_password:
                self.mqtt_client.username_pw_set(self.mqtt_user, self.mqtt_password)

            self.mqtt_client.on_connect = self._on_mqtt_connect
            self.mqtt_client.on_disconnect = self._on_mqtt_disconnect
            self.mqtt_client.on_message = self._on_mqtt_message

            self.mqtt_client.connect(self.mqtt_host, self.mqtt_port, 60)
            self.mqtt_client.loop_start()
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            return False

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties):
        """Callback for MQTT connection."""
        # Convert reason_code to int for backward compatibility
        rc = reason_code.value if hasattr(reason_code, 'value') else reason_code
        if rc == 0:
            logger.info(f"Connected to MQTT broker at {self.mqtt_host}:{self.mqtt_port}")
            # Subscribe to command topics for all devices
            command_topic = f"{self.device_prefix}/+/command"
            client.subscribe(command_topic)
            logger.info(f"Subscribed to command topic: {command_topic}")

            # Subscribe to global keep-alive control
            keepalive_topic = f"{self.device_prefix}/keepalive/command"
            client.subscribe(keepalive_topic)
            logger.info(f"Subscribed to keep-alive control topic: {keepalive_topic}")
        else:
            error_messages = {
                1: "Connection refused - incorrect protocol version",
                2: "Connection refused - invalid client identifier",
                3: "Connection refused - server unavailable",
                4: "Connection refused - bad username or password",
                5: "Connection refused - not authorized"
            }
            error_msg = error_messages.get(rc, f"Unknown error code {rc}")
            logger.error(f"MQTT connection failed: {error_msg}")
            if rc == 5:
                logger.error("Check your MQTT username and password in the .env file")

    def _on_mqtt_disconnect(self, client, userdata, flags, reason_code, properties):
        """Callback for MQTT disconnection."""
        # Convert reason_code to int for backward compatibility
        rc = reason_code.value if hasattr(reason_code, 'value') else reason_code
        if rc != 0:
            logger.warning("Unexpected MQTT disconnection")

    def _on_mqtt_message(self, client, userdata, msg):
        """Handle incoming MQTT command messages."""
        try:
            topic_parts = msg.topic.split('/')
            if len(topic_parts) >= 3 and topic_parts[-1] == 'command':
                # Handle keep-alive control commands
                if topic_parts[-2] == 'keepalive':
                    command_data = json.loads(msg.payload.decode())
                    if command_data.get('command') == 'keepalive_toggle':
                        enabled = command_data.get('enabled', False)
                        self._keepalive_enabled = enabled
                        logger.info(f"🔄 Keep-alive {'enabled' if enabled else 'disabled'} via MQTT command")

                        # Publish the new state back
                        state_topic = f"{self.device_prefix}/keepalive/state"
                        self.mqtt_client.publish(state_topic, json.dumps({
                            "keepalive_enabled": enabled,
                            "last_updated": datetime.now().isoformat()
                        }))
                        return

                # Handle device commands
                device_sn = topic_parts[-2]
                command_data = json.loads(msg.payload.decode())
                logger.info(f"🎛️ Received command for {device_sn}: {command_data}")

                # Store the command for processing in the main loop
                self._pending_commands.append((device_sn, command_data))
                self._command_queue_timestamp = time.time()
                logger.info(f"🎛️ Command added to pending queue. Queue size: {len(self._pending_commands)}")
                # Signal that commands are available for processing
                if self._command_event:
                    event_was_set = self._command_event.is_set()
                    self._command_event.set()
                    logger.info(f"🎛️ Command event set (was_set={event_was_set}) - main loop should process immediately")
                else:
                    logger.error("🎛️ Command event is None - commands cannot be processed!")

        except Exception as e:
            logger.error(f"Error handling MQTT message: {e}")

    async def _handle_device_command(self, device_sn: str, command_data: dict):
        """Handle device control commands."""
        try:
            logger.info(f"🔄 Processing command for {device_sn}: {command_data}")

            command = command_data.get('command')
            if not command:
                logger.error(f"No command specified in payload: {command_data}")
                return
            logger.info(f"🔄 Command extracted: {command}")

            # Get device info
            device_info = self.api.devices.get(device_sn) if self.api else None
            if not device_info:
                logger.error(f"Device not found: {device_sn}")
                return
            logger.info(f"🔄 Device found: {device_info.get('name', device_sn)}")

            # Only handle commands for C1000X devices
            if device_info.get('device_pn') != 'A1761':
                logger.error(f"Device {device_sn} does not support MQTT commands")
                return
            logger.info(f"🔄 Device model validated: A1761")

            # Check if MQTT session is connected
            if not self.api.mqttsession:
                logger.error(f"❌ No MQTT session available for command: {command}")
                return

            if not self.api.mqttsession.is_connected():
                logger.warning(f"🔄 MQTT session not connected, retrying command in 5 seconds...")
                await asyncio.sleep(5)
                # Try once more
                if not self.api.mqttsession.is_connected():
                    logger.error(f"❌ MQTT session still not connected, command failed: {command}")
                    return
            logger.info(f"🔄 MQTT session is connected")

            # Create device control instance
            logger.info(f"🔄 Creating MQTT device control instance...")
            device = create_mqtt_device(self.api, device_sn)
            if not device:
                logger.error(f"Failed to create MQTT device instance for {device_sn}")
                return
            logger.info(f"🔄 Device control instance created: {type(device)}")

            # Execute command
            result = False
            logger.info(f"🔄 Executing command: {command}")

            if command == "ac_output":
                enabled = command_data.get('enabled', False)
                logger.info(f"🔄 Calling device.set_ac_output(enabled={enabled})")
                result = await device.set_ac_output(enabled=enabled)
                logger.info(f"✅ AC output {'enabled' if enabled else 'disabled'} for {device_sn}")

            elif command == "dc_output":
                enabled = command_data.get('enabled', False)
                logger.info(f"🔄 Calling device.set_dc_output(enabled={enabled})")
                result = await device.set_dc_output(enabled=enabled)
                logger.info(f"✅ 12V DC output {'enabled' if enabled else 'disabled'} for {device_sn}")

            elif command == "ultrafast_charging":
                enabled = command_data.get('enabled', False)

                # Validate prerequisites for UltraFast charging - only works with AC power
                if enabled:
                    mqtt_data = device_info.get("mqtt_data", {})
                    grid_charging = mqtt_data.get("grid_to_battery_power", 0)

                    if float(grid_charging or 0) == 0:
                        logger.warning(f"❌ Cannot enable UltraFast charging: No AC power input detected")
                        logger.info(f"   Current AC charging: {grid_charging}W (UltraFast requires AC power)")
                        return

                    logger.info(f"✅ AC power detected ({grid_charging}W) - UltraFast charging can be enabled")

                logger.info(f"🔄 Calling device.set_ultrafast_charging(enabled={enabled})")
                result = await device.set_ultrafast_charging(enabled=enabled)
                logger.info(f"✅ UltraFast charging {'enabled' if enabled else 'disabled'} for {device_sn}")

            else:
                logger.error(f"Unknown command: {command}")
                return

            logger.info(f"🔄 Command execution result: {result}")

            # Update device state if command was successful
            if result:
                logger.info(f"🔄 Command successful, triggering state refresh...")
                # Wait a moment for the device to update its state
                await asyncio.sleep(1)
                # Trigger a state refresh
                if self.api.mqttsession:
                    self.api.mqttsession.realtime_trigger(deviceDict=device_info)
                    logger.info(f"🔄 Realtime trigger sent")

        except Exception as e:
            logger.error(f"❌ Error executing command for {device_sn}: {e}", exc_info=True)

    async def _process_pending_commands(self):
        """Process any pending MQTT commands."""
        if self._pending_commands:
            pending = self._pending_commands.copy()
            self._pending_commands.clear()
            logger.info(f"🎛️ Processing {len(pending)} pending commands")

            for device_sn, command_data in pending:
                logger.info(f"🎛️ Processing command for device {device_sn}: {command_data}")
                await self._handle_device_command(device_sn, command_data)

            logger.debug("🎛️ Command processing completed")
        else:
            logger.debug("🎛️ No pending commands to process")

        # Always clear the event after checking for commands
        if self._command_event:
            self._command_event.clear()
            logger.debug("🎛️ Command event cleared")

    async def _handle_keepalive_triggers(self):
        """Send keep-alive realtime triggers if enabled."""
        if not self._keepalive_enabled or not self.api or not self.api.mqttsession:
            return

        if not self.api.mqttsession.is_connected():
            logger.debug("MQTT session not connected, skipping keep-alive triggers")
            return

        current_time = asyncio.get_event_loop().time()
        keepalive_interval = 300  # 5 minutes default

        # Check if it's time to send keep-alive triggers
        if current_time - self._last_keepalive >= keepalive_interval:
            logger.info("🔄 Sending keep-alive triggers to maintain MQTT session activity")

            # Send realtime triggers to all MQTT-enabled devices
            mqtt_devices = [dev for dev in self.api.devices.values() if dev.get("mqtt_described")]

            for dev in mqtt_devices:
                try:
                    device_name = dev.get('name', 'Unknown')
                    device_sn = dev.get('device_sn', 'Unknown')

                    # Send occasional display wake-up for C1000X devices (every 6th keep-alive ~30min)
                    if dev.get('device_pn') == 'A1761' and (current_time % 1800) < keepalive_interval:  # Every ~30 minutes
                        logger.debug(f"💡 Keep-alive: Sending display wake-up for {device_name}...")
                        try:
                            device = create_mqtt_device(self.api, device_sn)
                            if device:
                                wake_result = await device.set_display(enabled=True)
                                if wake_result:
                                    logger.debug(f"✅ Keep-alive: Display wake-up successful for {device_name}")
                                await asyncio.sleep(1)  # Brief pause after wake-up
                        except Exception as e:
                            logger.debug(f"⚠️ Keep-alive: Display wake-up error for {device_name}: {e}")

                    resp = self.api.mqttsession.realtime_trigger(
                        deviceDict=dev, timeout=30
                    )

                    if resp and resp.is_published():
                        logger.debug(f"✅ Keep-alive trigger sent to {device_name}")
                    else:
                        logger.warning(f"❌ Keep-alive trigger failed for {device_name}")

                except Exception as e:
                    logger.warning(f"Keep-alive trigger error for {dev.get('name', 'Unknown')}: {e}")

            self._last_keepalive = current_time
            logger.info(f"🔄 Keep-alive cycle completed for {len(mqtt_devices)} devices")

    async def _check_and_recover_stale_commands(self):
        """Check for stale commands and restart MQTT session if needed."""
        if not self._pending_commands or not self._command_queue_timestamp:
            return

        current_time = time.time()
        time_since_commands_added = current_time - self._command_queue_timestamp

        if time_since_commands_added > self._max_command_wait_time:
            logger.warning(f"🚨 Commands have been pending for {time_since_commands_added:.1f}s (max: {self._max_command_wait_time}s)")
            logger.warning(f"🚨 {len(self._pending_commands)} commands stuck in queue - restarting MQTT session")

            # Log the stuck commands for debugging
            for i, (device_sn, command_data) in enumerate(self._pending_commands[:3]):  # Log first 3
                logger.warning(f"   📋 Stuck command {i+1}: {device_sn} -> {command_data}")

            await self._restart_mqtt_session()

    async def _restart_mqtt_session(self):
        """Restart the MQTT session to recover from stale state."""
        try:
            logger.info("🔄 Restarting MQTT session to recover from stale commands...")

            # Stop current MQTT session
            if self.api and self.api.mqttsession:
                self.api.stopMqttSession()
                await asyncio.sleep(2)  # Give it time to clean up

            # Restart MQTT session
            if self.api:
                mqtt_session = await self.api.startMqttSession()
                if mqtt_session:
                    # Re-subscribe to devices
                    for dev in self.api.devices.values():
                        if dev.get("mqtt_described"):
                            topic = f"{mqtt_session.get_topic_prefix(deviceDict=dev)}#"
                            resp = mqtt_session.subscribe(topic)
                            if resp and resp.is_failure:
                                logger.warning(f"Failed resubscription for topic: {topic}")

                    # Set up callback again
                    self.api.mqttsession.message_callback(self.mqtt_message_callback)

                    # Send realtime triggers to wake up devices
                    await asyncio.sleep(3)  # Wait for connection to stabilize

                    mqtt_devices = [dev for dev in self.api.devices.values() if dev.get("mqtt_described")]
                    for dev in mqtt_devices:
                        try:
                            device_name = dev.get('name', 'Unknown')
                            device_sn = dev.get('device_sn', 'Unknown')

                            # Send display wake-up command for C1000X devices first
                            if dev.get('device_pn') == 'A1761':  # C1000X
                                logger.info(f"💡 Recovery: Sending display wake-up for {device_name}...")
                                try:
                                    device = create_mqtt_device(self.api, device_sn)
                                    if device:
                                        wake_result = await device.set_display(enabled=True)
                                        if wake_result:
                                            logger.info(f"✅ Recovery: Display wake-up successful for {device_name}")
                                        else:
                                            logger.warning(f"⚠️ Recovery: Display wake-up failed for {device_name}")
                                        # Give device time to process wake-up command
                                        await asyncio.sleep(1)
                                except Exception as e:
                                    logger.warning(f"⚠️ Recovery: Display wake-up error for {device_name}: {e}")

                            resp = self.api.mqttsession.realtime_trigger(deviceDict=dev, timeout=30)
                            if resp and resp.is_published():
                                logger.info(f"🔔 Recovery trigger sent to {device_name}")
                            else:
                                logger.warning(f"❌ Recovery trigger failed for {device_name}")
                        except Exception as e:
                            logger.warning(f"Recovery trigger error for {dev.get('name', 'Unknown')}: {e}")

                    # Clear stuck commands and reset timestamp
                    stuck_count = len(self._pending_commands)
                    self._pending_commands.clear()
                    self._command_queue_timestamp = 0

                    # Reset command event
                    if self._command_event:
                        self._command_event.clear()

                    logger.info(f"✅ MQTT session restarted successfully, cleared {stuck_count} stuck commands")

                else:
                    logger.error("❌ Failed to restart MQTT session")

        except Exception as e:
            logger.error(f"❌ Error during MQTT session restart: {e}", exc_info=True)

    def publish_discovery_config(self, device_sn: str, device_info: dict, entity_type: str,
                               entity_name: str, entity_config: dict):
        """Publish Home Assistant MQTT discovery configuration."""
        if not self.mqtt_client:
            logger.debug("MQTT client not available, skipping discovery config")
            return False

        discovery_key = f"{device_sn}_{entity_name}"
        if discovery_key in self.published_discoveries:
            return True  # Already published

        # Create device information for HA with additional attributes
        device_ha_info = {
            "identifiers": [f"{self.device_prefix}_{device_sn}"],
            "name": f"{device_info.get('name', f'Anker {device_info.get('device_pn', 'Unknown')}')} (MQTT)",
            "model": device_info.get('device_pn', 'Unknown'),
            "manufacturer": "Anker Solix",
            "sw_version": device_info.get('sw_version', 'Unknown'),
            "serial_number": device_sn,
        }

        # Add optional device info fields if available
        if device_info.get('wifi_mac'):
            device_ha_info["connections"] = [["mac", device_info.get('wifi_mac')]]
        if device_info.get('hw_version'):
            device_ha_info["hw_version"] = device_info.get('hw_version')
        if device_info.get('suggested_area'):
            device_ha_info["suggested_area"] = device_info.get('suggested_area')

        # Add device attributes for static/config information
        device_attributes = {
            "device_sn": device_sn,
            "device_timeout_minutes": device_info.get('device_timeout_minutes'),
            "display_timeout_seconds": device_info.get('display_timeout_seconds'),
            "controller_firmware": device_info.get('sw_controller'),
            "expansion_firmware": device_info.get('sw_expansion'),
            "expansion_type": device_info.get('exp_1_type'),
            "wifi_mac": device_info.get('wifi_mac'),
            "bt_mac": device_info.get('bt_ble_mac'),
            "time_zone": device_info.get('time_zone'),
            "max_load": device_info.get('max_load'),
            "battery_size": device_info.get('battery_size'),
        }

        # Remove None values
        device_attributes = {k: v for k, v in device_attributes.items() if v is not None}

        # Only add via_device if it has a valid value
        if device_info.get('site_id'):
            device_ha_info["via_device"] = device_info.get('site_id')

        # Only add configuration_url if we want it
        # device_ha_info["configuration_url"] = f"http://localhost/anker_solix_mqtt/{device_sn}"

        # Create friendly sensor names
        friendly_names = {
            "wifi_signal": "WiFi Signal",
            "rssi": "WiFi Signal Strength",
            "battery_soc": "Battery Level",
            "battery_soc_total": "Total Battery Level",
            "battery_soh": "Battery Health",
            "battery_capacity": "Battery Capacity",
            "battery_energy": "Battery Energy",
            "battery_size": "Battery Size",
            "temperature": "Temperature",
            "exp_1_temperature": "Expansion Temperature",
            "ac_output_power": "AC Output",
            "ac_output_power_total": "Total AC Output",
            "dc_input_power": "Solar Input",
            "grid_to_battery_power": "AC Charging",
            "usbc_1_power": "USB-C 1",
            "usbc_2_power": "USB-C 2",
            "usba_1_power": "USB-A 1",
            "usba_2_power": "USB-A 2",
            "max_load": "Max Output",
            "exp_1_soc": "Expansion Battery",
            "exp_1_soh": "Expansion Battery Health",
            "expansion_packs": "Expansion Packs",
            "device_timeout_minutes": "Auto Shutdown",
            "display_timeout_seconds": "Screen Timeout",
            "exp_1_type": "Expansion Type",
            "sw_version": "Software Version",
            "sw_expansion": "Expansion Firmware",
            "sw_controller": "Controller Firmware",
            "status": "Status"
        }

        friendly_name = friendly_names.get(entity_name, entity_name.replace('_', ' ').title())

        # Base configuration
        config = {
            "name": friendly_name,
            "unique_id": f"{self.device_prefix}_{device_sn}_{entity_name}",
            "default_entity_id": f"{entity_type}.{self.device_prefix}_{device_sn}_{entity_name}",
            "device": device_ha_info,
            "state_topic": f"{self.device_prefix}/{device_sn}/state",
            "json_attributes_topic": f"{self.device_prefix}/{device_sn}/attributes",
            "availability_topic": f"{self.device_prefix}/{device_sn}/availability",
            "payload_available": "online",
            "payload_not_available": "offline"
        }

        # Merge with entity-specific configuration
        config.update(entity_config)

        # Publish discovery topic
        discovery_topic = f"{self.ha_discovery_prefix}/{entity_type}/{self.device_prefix}_{device_sn}/{entity_name}/config"

        try:
            result = self.mqtt_client.publish(discovery_topic, json.dumps(config), retain=True)
            if result.rc == mqtt_client.MQTT_ERR_SUCCESS:
                self.published_discoveries.add(discovery_key)
                logger.info(f"📡 Published HA discovery: {discovery_topic}")
                logger.debug(f"Discovery config: {json.dumps(config, indent=2)}")
                return True
            else:
                logger.error(f"Failed to publish discovery config: {result.rc}")
                return False
        except Exception as e:
            logger.error(f"Error publishing discovery config: {e}")
            return False

    def create_sensor_entities(self, device_sn: str, device_info: dict, data: dict):
        """Create sensor entities for a device based on available data."""
        device_type = device_info.get('type', '')

        # Common sensors for all devices
        common_sensors = {
            "wifi_signal": {
                "unit_of_measurement": "%",
                "value_template": "{{ value_json.wifi_signal }}",
                "icon": "mdi:wifi",
                "state_class": "measurement"
            },
            "status": {
                "value_template": "{{ value_json.status_desc }}",
                "icon": "mdi:information"
            }
        }

        # Device-specific sensors
        device_sensors = {}

        if device_type == SolixDeviceType.SOLARBANK.value:
            device_sensors.update({
                "battery_soc": {
                    "device_class": "battery",
                    "unit_of_measurement": "%",
                    "value_template": "{{ value_json.battery_soc }}",
                    "state_class": "measurement"
                },
                "battery_capacity": {
                    "device_class": "energy",
                    "unit_of_measurement": "Wh",
                    "value_template": "{{ value_json.battery_capacity }}",
                    "icon": "mdi:battery-capacity-variant"
                },
                "input_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.input_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:solar-panel"
                },
                "output_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.output_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:transmission-tower"
                },
                "charging_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.charging_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:battery-charging"
                },
                "power_limit": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.power_limit }}",
                    "icon": "mdi:speedometer"
                }
            })

        elif device_type == SolixDeviceType.SMARTMETER.value:
            device_sensors.update({
                "grid_import_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.grid_to_home_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:transmission-tower-import"
                },
                "grid_export_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.photovoltaic_to_grid_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:transmission-tower-export"
                },
                "grid_status": {
                    "value_template": "{{ value_json.grid_status_desc }}",
                    "icon": "mdi:electric-switch"
                }
            })

        elif device_type == SolixDeviceType.SMARTPLUG.value:
            device_sensors.update({
                "current_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.current_power }}",
                    "state_class": "measurement"
                },
                "energy_today": {
                    "device_class": "energy",
                    "unit_of_measurement": "kWh",
                    "value_template": "{{ value_json.energy_today }}",
                    "state_class": "total_increasing"
                }
            })

        elif device_type == SolixDeviceType.INVERTER.value:
            device_sensors.update({
                "generate_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.generate_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:solar-panel"
                }
            })

        elif device_type in [SolixDeviceType.POWERPANEL.value, SolixDeviceType.HES.value]:
            device_sensors.update({
                "battery_capacity": {
                    "device_class": "energy",
                    "unit_of_measurement": "Wh",
                    "value_template": "{{ value_json.battery_capacity }}",
                    "icon": "mdi:battery-capacity-variant"
                },
                "battery_energy": {
                    "device_class": "energy",
                    "unit_of_measurement": "Wh",
                    "value_template": "{{ value_json.battery_energy }}",
                    "state_class": "measurement",
                    "icon": "mdi:battery"
                }
            })

        elif device_type == SolixDeviceType.PPS.value:
            # Portable Power Station (like C1000) specific sensors
            device_sensors.update({
                "battery_soc": {
                    "device_class": "battery",
                    "unit_of_measurement": "%",
                    "value_template": "{{ value_json.battery_soc }}",
                    "state_class": "measurement"
                },
                "battery_capacity": {
                    "device_class": "energy",
                    "unit_of_measurement": "Wh",
                    "value_template": "{{ value_json.battery_capacity }}",
                    "icon": "mdi:battery-capacity-variant"
                },
                "battery_energy": {
                    "device_class": "energy",
                    "unit_of_measurement": "Wh",
                    "value_template": "{{ value_json.battery_energy }}",
                    "state_class": "measurement",
                    "icon": "mdi:battery"
                },
                "ac_output_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.ac_output_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:power-plug"
                },
                "ac_output_power_total": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.ac_output_power_total }}",
                    "state_class": "measurement",
                    "icon": "mdi:power-plug"
                },
                "dc_input_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.dc_input_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:solar-panel"
                },
                "grid_to_battery_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.grid_to_battery_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:battery-charging"
                },
                "usbc_1_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.usbc_1_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:usb"
                },
                "usbc_2_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.usbc_2_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:usb"
                },
                "usba_1_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.usba_1_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:usb"
                },
                "usba_2_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.usba_2_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:usb"
                },
                "temperature": {
                    "device_class": "temperature",
                    "unit_of_measurement": "°C",
                    "value_template": "{{ value_json.temperature }}",
                    "state_class": "measurement"
                },
                "battery_soh": {
                    "unit_of_measurement": "%",
                    "value_template": "{{ value_json.battery_soh }}",
                    "state_class": "measurement",
                    "icon": "mdi:battery-heart"
                },
                "battery_soc_total": {
                    "device_class": "battery",
                    "unit_of_measurement": "%",
                    "value_template": "{% if value_json.battery_soc_total is defined and value_json.battery_soc_total is not none %}{{ value_json.battery_soc_total }}{% elif value_json.exp_1_soc is defined and value_json.battery_soc is defined %}{{ ((value_json.battery_soc|int + value_json.exp_1_soc|int) / 2)|round }}{% else %}{{ value_json.battery_soc }}{% endif %}",
                    "state_class": "measurement"
                },
                "exp_1_soc": {
                    "device_class": "battery",
                    "unit_of_measurement": "%",
                    "value_template": "{{ value_json.exp_1_soc }}",
                    "state_class": "measurement",
                    "icon": "mdi:battery-plus-variant"
                }
            })

        # Add generic sensors for any device that might have power data
        if any(key in data for key in ['current_power', 'input_power', 'output_power', 'charging_power']):
            device_sensors.update({
                "current_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.current_power }}",
                    "state_class": "measurement"
                }
            })

        # Publish all sensor configurations
        all_sensors = {**common_sensors, **device_sensors}

        # Filter out expansion sensors if no expansion pack installed
        expansion_packs = data.get('expansion_packs', 0)
        if not (expansion_packs and int(expansion_packs) > 0):
            # Remove expansion-only sensors when no expansion installed
            expansion_sensor_names = {'exp_1_soc', 'exp_1_soh', 'exp_1_temperature', 'expansion_packs', 'exp_1_type', 'sw_expansion'}
            all_sensors = {k: v for k, v in all_sensors.items() if k not in expansion_sensor_names}
            logger.debug(f"🚫 Filtered out expansion sensors (no expansion installed)")

        published_count = 0
        for sensor_name, sensor_config in all_sensors.items():
            # Check if the data exists in either the main data or in value_template references
            has_data = sensor_name in data
            if not has_data and 'value_template' in sensor_config:
                # Extract the field name from value_template like "{{ value_json.battery_soc }}"
                import re
                template_match = re.search(r'value_json\.(\w+)', sensor_config['value_template'])
                if template_match:
                    field_name = template_match.group(1)
                    has_data = field_name in data
                    if has_data:
                        logger.debug(f"  ✓ Found data for {sensor_name} -> {field_name}: {data.get(field_name)}")
                    else:
                        logger.debug(f"  ✗ No data for {sensor_name} -> {field_name}")
            else:
                if has_data:
                    logger.debug(f"  ✓ Direct match for {sensor_name}: {data.get(sensor_name)}")
                else:
                    logger.debug(f"  ✗ No direct data for {sensor_name}")

            if has_data:
                self.publish_discovery_config(
                    device_sn, device_info, "sensor", sensor_name, sensor_config
                )
                published_count += 1
            else:
                logger.debug(f"  🚫 Skipping {sensor_name} - no data available")

        logger.info(f"📊 Published {published_count} sensor entities for {device_info.get('name', device_sn)}")

        # Show which sensors were created
        created_sensors = []
        for sensor_name, sensor_config in all_sensors.items():
            has_data = sensor_name in data
            if not has_data and 'value_template' in sensor_config:
                import re
                template_match = re.search(r'value_json\.(\w+)', sensor_config['value_template'])
                if template_match:
                    field_name = template_match.group(1)
                    has_data = field_name in data
            if has_data:
                created_sensors.append(sensor_name)

        if created_sensors:
            logger.info(f"  ✅ Created sensors: {created_sensors}")
        else:
            logger.warning(f"  ❌ No sensors created - check data field matching")

    def create_switch_entities(self, device_sn: str, device_info: dict, data: dict):
        """Create switch entities for device controls."""
        device_type = device_info.get('type', '')
        switches = {}

        # Only add switches for PPS devices (C1000X) that support MQTT control
        if device_type == SolixDeviceType.PPS.value and device_info.get('device_pn') == 'A1761':
            switches.update({
                "ac_output": {
                    "name": "AC Output",
                    "icon": "mdi:power-socket-eu",
                    "device_class": "outlet",
                    "command_topic": f"{self.device_prefix}/{device_sn}/command",
                    "state_topic": f"{self.device_prefix}/{device_sn}/state",
                    "value_template": "{% if value_json.ac_output_power_switch == 1 %}ON{% else %}OFF{% endif %}",
                    "payload_on": '{"command": "ac_output", "enabled": true}',
                    "payload_off": '{"command": "ac_output", "enabled": false}',
                    "state_on": "ON",
                    "state_off": "OFF"
                },
                "dc_12v_output": {
                    "name": "12V DC Output",
                    "icon": "mdi:car-battery",
                    "command_topic": f"{self.device_prefix}/{device_sn}/command",
                    "state_topic": f"{self.device_prefix}/{device_sn}/state",
                    "value_template": "{% if value_json.dc_output_power_switch == 1 %}ON{% else %}OFF{% endif %}",
                    "payload_on": '{"command": "dc_output", "enabled": true}',
                    "payload_off": '{"command": "dc_output", "enabled": false}',
                    "state_on": "ON",
                    "state_off": "OFF"
                },
                "ultrafast_charging": {
                    "name": "UltraFast Charging",
                    "icon": "mdi:flash",
                    "command_topic": f"{self.device_prefix}/{device_sn}/command",
                    "state_topic": f"{self.device_prefix}/{device_sn}/state",
                    "value_template": "{% if value_json.ultrafast_charging == 1 %}ON{% else %}OFF{% endif %}",
                    "payload_on": '{"command": "ultrafast_charging", "enabled": true}',
                    "payload_off": '{"command": "ultrafast_charging", "enabled": false}',
                    "state_on": "ON",
                    "state_off": "OFF",
                    "entity_category": "config"
                }
            })

        # Force republish of switches by clearing cache first
        switches_to_clear = [f"{device_sn}_ac_output", f"{device_sn}_dc_12v_output", f"{device_sn}_ultrafast_charging"]
        for switch_key in switches_to_clear:
            self.published_discoveries.discard(switch_key)

        # Publish switch configurations
        for switch_name, switch_config in switches.items():
            self.publish_discovery_config(
                device_sn, device_info, "switch", switch_name, switch_config
            )

    def create_binary_sensor_entities(self, device_sn: str, device_info: dict, data: dict):
        """Create binary sensor entities for a device."""
        binary_sensors = {}

        if "wifi_online" in data:
            binary_sensors["wifi_connectivity"] = {
                "device_class": "connectivity",
                "value_template": "{{ value_json.wifi_online }}",
                "payload_on": True,
                "payload_off": False,
                "entity_category": "diagnostic"
            }

        if "is_admin" in data:
            binary_sensors["admin_access"] = {
                "value_template": "{{ value_json.is_admin }}",
                "payload_on": True,
                "payload_off": False,
                "icon": "mdi:shield-account",
                "entity_category": "diagnostic"
            }

        # Publish binary sensor configurations
        for sensor_name, sensor_config in binary_sensors.items():
            # Check if we have the required field for this binary sensor
            import re
            template = sensor_config.get("value_template", "")
            field_match = re.search(r'value_json\.(\w+)', template)
            if field_match and field_match.group(1) in data:
                self.publish_discovery_config(
                    device_sn, device_info, "binary_sensor", sensor_name, sensor_config
                )

    def create_keepalive_switch(self):
        """Create global keep-alive switch for controlling MQTT session activity."""
        if not self.mqtt_client:
            logger.debug("MQTT client not available, skipping keep-alive switch creation")
            return

        # Create a virtual device for global controls
        global_device_info = {
            "identifiers": [f"{self.device_prefix}_global"],
            "name": f"Anker Solix MQTT Publisher",
            "model": "MQTT Publisher",
            "manufacturer": "Anker Solix API",
            "sw_version": "1.0",
        }

        switch_config = {
            "name": "Keep-Alive Mode",
            "icon": "mdi:heart-pulse",
            "device_class": "switch",
            "command_topic": f"{self.device_prefix}/keepalive/command",
            "state_topic": f"{self.device_prefix}/keepalive/state",
            "value_template": "{% if value_json.keepalive_enabled %}ON{% else %}OFF{% endif %}",
            "payload_on": '{"command": "keepalive_toggle", "enabled": true}',
            "payload_off": '{"command": "keepalive_toggle", "enabled": false}',
            "state_on": "ON",
            "state_off": "OFF",
            "entity_category": "config",
            "device": global_device_info,
            "unique_id": f"{self.device_prefix}_global_keepalive",
            "default_entity_id": f"switch.{self.device_prefix}_keepalive_mode",
            "availability_topic": f"{self.device_prefix}/global/availability",
            "payload_available": "online",
            "payload_not_available": "offline"
        }

        # Publish discovery topic
        discovery_topic = f"{self.ha_discovery_prefix}/switch/{self.device_prefix}_global/keepalive/config"

        try:
            result = self.mqtt_client.publish(discovery_topic, json.dumps(switch_config), retain=True)
            if result.rc == mqtt_client.MQTT_ERR_SUCCESS:
                logger.info(f"🔄 Published keep-alive switch discovery: {discovery_topic}")

                # Publish initial state and availability
                self.mqtt_client.publish(f"{self.device_prefix}/global/availability", "online", retain=True)
                initial_state = {
                    "keepalive_enabled": self._keepalive_enabled,
                    "last_updated": datetime.now().isoformat()
                }
                self.mqtt_client.publish(f"{self.device_prefix}/keepalive/state", json.dumps(initial_state), retain=True)
                return True
            else:
                logger.error(f"Failed to publish keep-alive switch discovery: {result.rc}")
                return False
        except Exception as e:
            logger.error(f"Error publishing keep-alive switch discovery: {e}")
            return False

    def publish_device_state(self, device_sn: str, device_info: dict, mqtt_data: dict = None):
        """Publish device state data to MQTT."""
        if not self.mqtt_client:
            logger.debug("MQTT client not available, skipping state publishing")
            return False

        # Combine API data with MQTT data if available
        state_data = dict(device_info)
        if mqtt_data:
            # Merge MQTT data, preferring real-time MQTT values
            state_data.update(mqtt_data)

        # Add switch state fields based on actual device state from MQTT data
        if device_info.get('device_pn') == 'A1761':  # C1000X
            # Read actual switch states from MQTT data instead of hardcoding
            logger.debug(f"🔍 Available state fields: {list(state_data.keys())}")

            # Get actual switch states from MQTT data (these are the real device states)
            ac_switch_state = state_data.get('ac_output_power_switch')
            dc_switch_state = state_data.get('dc_output_power_switch')
            ultrafast_state = state_data.get('ultrafast_charging', 0)

            # Only override if we don't have the switch states in MQTT data
            if ac_switch_state is None:
                # Fallback: If we don't have switch state, infer from power consumption
                ac_power = float(state_data.get('ac_output_power', 0) or 0)
                state_data['ac_output_power_switch'] = 1 if ac_power > 0 else 0
                logger.debug(f"🔧 Inferring AC switch from power: {ac_power}W -> state={state_data['ac_output_power_switch']}")
            else:
                # Use actual switch state from MQTT
                state_data['ac_output_power_switch'] = int(ac_switch_state)
                logger.debug(f"🔧 AC switch from MQTT: {ac_switch_state}")

            if dc_switch_state is None:
                # Fallback: Default to OFF if no data
                state_data['dc_output_power_switch'] = 0
                logger.debug(f"🔧 DC switch defaulted to: 0")
            else:
                # Use actual switch state from MQTT
                state_data['dc_output_power_switch'] = int(dc_switch_state)
                logger.debug(f"🔧 DC switch from MQTT: {dc_switch_state}")

            # Set ultrafast charging state
            state_data['ultrafast_charging'] = ultrafast_state

            logger.info(f"🔧 Switch states: AC={state_data.get('ac_output_power_switch', 'N/A')}, DC={state_data.get('dc_output_power_switch', 'N/A')}, Ultra={state_data['ultrafast_charging']}")

        # Add timestamp
        state_data["last_updated"] = datetime.now().isoformat()

        # Publish availability
        availability_topic = f"{self.device_prefix}/{device_sn}/availability"
        self.mqtt_client.publish(availability_topic, "online", retain=True)

        # Prepare device attributes for the attributes topic
        device_attributes = {
            "device_sn": device_sn,
            "device_timeout_minutes": device_info.get('device_timeout_minutes'),
            "display_timeout_seconds": device_info.get('display_timeout_seconds'),
            "controller_firmware": device_info.get('sw_controller'),
            "expansion_firmware": device_info.get('sw_expansion'),
            "expansion_type": device_info.get('exp_1_type'),
            "wifi_mac": device_info.get('wifi_mac'),
            "bt_mac": device_info.get('bt_ble_mac'),
            "time_zone": device_info.get('time_zone'),
            "max_load": device_info.get('max_load'),
            "battery_size": device_info.get('battery_size'),
            "sw_version": device_info.get('sw_version'),
        }

        # Remove None values from attributes
        device_attributes = {k: v for k, v in device_attributes.items() if v is not None}

        # Publish device attributes
        attributes_topic = f"{self.device_prefix}/{device_sn}/attributes"
        try:
            result = self.mqtt_client.publish(attributes_topic, json.dumps(device_attributes))
            if result.rc == mqtt_client.MQTT_ERR_SUCCESS:
                logger.debug(f"📋 Published device attributes to {attributes_topic}: {list(device_attributes.keys())}")
            else:
                logger.error(f"Failed to publish attributes: {result.rc}")
        except Exception as e:
            logger.error(f"Error publishing device attributes: {e}")

        # Publish state
        state_topic = f"{self.device_prefix}/{device_sn}/state"
        try:
            result = self.mqtt_client.publish(state_topic, json.dumps(state_data))
            if result.rc == mqtt_client.MQTT_ERR_SUCCESS:
                logger.info(f"📤 Published state to {state_topic}")
                logger.debug(f"🔋 State values: battery_soc={state_data.get('battery_soc', 'N/A')}, ac_output_power={state_data.get('ac_output_power', 'N/A')}, temperature={state_data.get('temperature', 'N/A')}")
                return True
            else:
                logger.error(f"Failed to publish state: {result.rc}")
                return False
        except Exception as e:
            logger.error(f"Error publishing device state: {e}")
            return False

    def cleanup_unused_discovery_entries(self, device_sn: str, current_sensors: set):
        """Clean up unused MQTT discovery entries for removed sensors."""
        if not self.mqtt_client:
            return

        # List of sensors that were moved to device attributes
        removed_sensors = {
            "device_timeout_minutes", "display_timeout_seconds", "sw_version",
            "sw_controller", "sw_expansion", "exp_1_type", "wifi_mac",
            "bt_mac", "time_zone", "battery_size", "max_load"
        }

        # Clean up removed sensors by publishing empty retained messages
        for sensor_name in removed_sensors:
            discovery_topic = f"{self.ha_discovery_prefix}/sensor/{self.device_prefix}_{device_sn}/{sensor_name}/config"
            try:
                self.mqtt_client.publish(discovery_topic, "", retain=True)
                logger.info(f"🧹 Cleaned up discovery entry: {sensor_name}")
            except Exception as e:
                logger.error(f"Error cleaning up discovery entry {sensor_name}: {e}")

        # Force refresh of existing entities to apply new categories
        logger.info(f"🔄 Forcing refresh of entity categories for {device_sn}")
        self.published_discoveries.clear()  # Clear cache to force republishing

    def _cleanup_expansion_sensors(self, device_sn: str):
        """Clean up expansion sensor discovery entries when no expansion is installed."""
        if not self.mqtt_client:
            return

        expansion_sensors = {
            'exp_1_soc', 'exp_1_soh', 'exp_1_temperature', 'expansion_packs', 'exp_1_type', 'sw_expansion'
        }

        for sensor_name in expansion_sensors:
            discovery_topic = f"{self.ha_discovery_prefix}/sensor/{self.device_prefix}_{device_sn}/{sensor_name}/config"
            try:
                self.mqtt_client.publish(discovery_topic, "", retain=True)
                logger.info(f"🧹 Removed expansion sensor: {sensor_name}")
            except Exception as e:
                logger.error(f"Error cleaning up expansion sensor {sensor_name}: {e}")

    def update_sensors_from_mqtt_data(self, device_sn: str, device_info: dict):
        """Update/create sensors when MQTT data is available."""
        # Always check session cache first as it contains the most up-to-date data
        session_data = {}
        if self.api.mqttsession and hasattr(self.api.mqttsession, 'mqtt_data'):
            session_data = self.api.mqttsession.mqtt_data.get(device_sn, {})

        # Use session data as primary source, fall back to device mqtt_data
        mqtt_data = session_data if session_data else device_info.get("mqtt_data", {})

        if not mqtt_data:
            logger.info(f"❌ No MQTT data available for {device_sn}")
            return

        if session_data:
            logger.info(f"📡 Using session cache data: {len(session_data)} fields")
            # Update device_info for state publishing
            device_info["mqtt_data"] = mqtt_data
        else:
            logger.info(f"📡 Using device mqtt_data: {len(mqtt_data)} fields")

        logger.info(f"📊 MQTT data sample: {dict(list(mqtt_data.items())[:5])}")

        # Combine device info with MQTT data
        combined_data = dict(device_info)
        combined_data.update(mqtt_data)

        logger.info(f"🔄 Processing {device_info.get('name', device_sn)} with {len(mqtt_data)} MQTT fields")
        logger.info(f"  📡 MQTT fields: {list(mqtt_data.keys())}")

        # Track existing entities for this device
        existing_entities = self.device_entities.get(device_sn, set())
        logger.info(f"  📊 Existing entities ({len(existing_entities)}): {existing_entities}")

        # Get all potential sensors for this device type
        device_type = device_info.get('type', '')
        all_possible_sensors = self._get_sensor_definitions(device_type)

        # Add expansion sensors only if expansion pack is installed
        # Enhanced debugging for expansion pack detection
        expansion_fields = {
            'expansion_packs': combined_data.get('expansion_packs'),
            'expansion_packs_a?': combined_data.get('expansion_packs_a?'),
            'expansion_packs_b?': combined_data.get('expansion_packs_b?'),
            'expansion_packs_c?': combined_data.get('expansion_packs_c?'),
            'exp_1_soc': combined_data.get('exp_1_soc'),
            'exp_1_soh': combined_data.get('exp_1_soh'),
            'exp_1_type': combined_data.get('exp_1_type'),
            'exp_1_temperature': combined_data.get('exp_1_temperature'),
            'battery_soc_total': combined_data.get('battery_soc_total'),
            'sw_expansion': combined_data.get('sw_expansion'),
        }

        logger.info(f"🔍 EXPANSION DEBUG - All expansion-related fields for {device_sn}:")
        for field, value in expansion_fields.items():
            if value is not None:
                logger.info(f"   📊 {field}: {value} (type: {type(value)})")
            else:
                logger.info(f"   ❌ {field}: None")

        expansion_packs = combined_data.get('expansion_packs', 0)
        # Also check alternative expansion pack fields
        alt_expansion_indicators = [
            combined_data.get('expansion_packs_a?'),
            combined_data.get('expansion_packs_b?'),
            combined_data.get('expansion_packs_c?')
        ]

        # Check if any expansion indicator suggests expansion pack is present
        has_expansion = False
        if expansion_packs and int(expansion_packs) > 0:
            has_expansion = True
            logger.info(f"🔋 EXPANSION DEBUG - Detected via 'expansion_packs': {expansion_packs}")
        elif any(val and int(val or 0) > 0 for val in alt_expansion_indicators if val is not None):
            has_expansion = True
            logger.info(f"🔋 EXPANSION DEBUG - Detected via alternative fields: {alt_expansion_indicators}")
        elif combined_data.get('exp_1_soc') is not None:
            has_expansion = True
            logger.info(f"🔋 EXPANSION DEBUG - Detected via exp_1_soc presence: {combined_data.get('exp_1_soc')}")
        elif combined_data.get('exp_1_type') is not None:
            has_expansion = True
            logger.info(f"🔋 EXPANSION DEBUG - Detected via exp_1_type presence: {combined_data.get('exp_1_type')}")

        logger.info(f"🔋 EXPANSION DEBUG - Final decision: has_expansion={has_expansion}")

        if has_expansion:
            expansion_sensors = {
                "battery_soc_total": {
                    "device_class": "battery",
                    "unit_of_measurement": "%",
                    "value_template": "{{ value_json.battery_soc_total }}",
                    "state_class": "measurement"
                },
                "battery_capacity": {
                    "device_class": "energy",
                    "unit_of_measurement": "Wh",
                    "value_template": "{{ value_json.battery_capacity }}",
                    "icon": "mdi:battery-capacity-variant"
                },
                "exp_1_soc": {
                    "device_class": "battery",
                    "unit_of_measurement": "%",
                    "value_template": "{{ value_json.exp_1_soc }}",
                    "state_class": "measurement",
                    "icon": "mdi:battery-plus-variant"
                },
                "exp_1_soh": {
                    "unit_of_measurement": "%",
                    "value_template": "{{ value_json.exp_1_soh }}",
                    "state_class": "measurement",
                    "icon": "mdi:battery-heart-variant"
                },
                "exp_1_temperature": {
                    "device_class": "temperature",
                    "unit_of_measurement": "°C",
                    "value_template": "{{ value_json.exp_1_temperature }}",
                    "state_class": "measurement"
                },
                "expansion_packs": {
                    "value_template": "{{ value_json.expansion_packs }}",
                    "icon": "mdi:battery-plus"
                },
                "exp_1_type": {
                    "value_template": "{{ value_json.exp_1_type }}",
                    "icon": "mdi:battery-plus-outline"
                },
                "sw_expansion": {
                    "value_template": "{{ value_json.sw_expansion }}",
                    "icon": "mdi:update"
                }
            }
            all_possible_sensors.update(expansion_sensors)
            logger.info(f"  🔋 EXPANSION DEBUG - Adding expansion sensors")
            logger.info(f"  🔋 EXPANSION DEBUG - Added sensor types: {list(expansion_sensors.keys())}")

            # Log which expansion sensors will actually have data
            for sensor_name, sensor_config in expansion_sensors.items():
                has_data = sensor_name in combined_data
                if not has_data and 'value_template' in sensor_config:
                    import re
                    template_match = re.search(r'value_json\.(\w+)', sensor_config['value_template'])
                    if template_match:
                        field_name = template_match.group(1)
                        has_data = field_name in combined_data
                        field_value = combined_data.get(field_name)
                        logger.info(f"     📊 {sensor_name} -> {field_name}: {field_value} (has_data: {has_data})")

                        # Special note for battery_soc_total
                        if sensor_name == "battery_soc_total" and field_name == "battery_soc_total" and not has_data:
                            main_soc = combined_data.get('battery_soc')
                            exp_soc = combined_data.get('exp_1_soc')
                            if main_soc is not None and exp_soc is not None:
                                calculated_total = round((int(main_soc) + int(exp_soc)) / 2)
                                logger.info(f"     ℹ️ Note: Device doesn't provide battery_soc_total field - will calculate from individual batteries")
                                logger.info(f"     ℹ️ Calculation: main={main_soc}% + expansion={exp_soc}% = total={calculated_total}% (equal 1056Wh capacities)")
                            else:
                                logger.info(f"     ℹ️ Note: Device doesn't provide battery_soc_total field - will show main battery_soc instead")
                                logger.info(f"     ℹ️ Individual batteries: main={main_soc}%, expansion={exp_soc}%")
                else:
                    field_value = combined_data.get(sensor_name)
                    logger.info(f"     📊 {sensor_name}: {field_value} (has_data: {has_data})")

        logger.info(f"  🎯 Checking {len(all_possible_sensors)} potential sensors for type '{device_type}'")
        logger.info(f"  📋 Available MQTT fields: {sorted(combined_data.keys())}")

        new_entities = set()
        for sensor_name, sensor_config in all_possible_sensors.items():
            # Check if we have data for this sensor
            has_data = sensor_name in combined_data
            field_name = sensor_name

            if not has_data and 'value_template' in sensor_config:
                import re
                template_match = re.search(r'value_json\.(\w+)', sensor_config['value_template'])
                if template_match:
                    field_name = template_match.group(1)
                    has_data = field_name in combined_data

            # Log what we're checking
            logger.debug(f"    Sensor '{sensor_name}' -> field '{field_name}': has_data={has_data}, already_exists={sensor_name in existing_entities}")

            # Create sensor if we have data and it doesn't exist yet
            if has_data and sensor_name not in existing_entities:
                field_value = combined_data.get(field_name, 'N/A')
                logger.info(f"  ✨ Creating sensor: {sensor_name} (field: {field_name} = {field_value})")

                success = self.publish_discovery_config(
                    device_sn, device_info, "sensor", sensor_name, sensor_config
                )
                if success:
                    new_entities.add(sensor_name)
                else:
                    logger.warning(f"  ❌ Failed to create sensor: {sensor_name}")

        # Update tracking
        if device_sn not in self.device_entities:
            self.device_entities[device_sn] = set()
        self.device_entities[device_sn].update(new_entities)

        if new_entities:
            logger.info(f"🎉 Added {len(new_entities)} new sensors: {new_entities}")
        else:
            logger.info("ℹ️ No new sensors added (all sensors already exist or no matching data)")

        # Clean up unused discovery entries (run once per device)
        cleanup_key = f"cleanup_{device_sn}"
        if not hasattr(self, '_cleanup_done'):
            self._cleanup_done = set()
        if cleanup_key not in self._cleanup_done:
            self.cleanup_unused_discovery_entries(device_sn, new_entities)

            # Also clean up expansion sensors if no expansion installed
            # Use the same enhanced detection logic as above
            cleanup_expansion_fields = {
                'expansion_packs': combined_data.get('expansion_packs'),
                'expansion_packs_a?': combined_data.get('expansion_packs_a?'),
                'expansion_packs_b?': combined_data.get('expansion_packs_b?'),
                'expansion_packs_c?': combined_data.get('expansion_packs_c?'),
                'exp_1_soc': combined_data.get('exp_1_soc'),
                'exp_1_type': combined_data.get('exp_1_type'),
            }

            expansion_packs = combined_data.get('expansion_packs', 0)
            alt_expansion_indicators = [
                combined_data.get('expansion_packs_a?'),
                combined_data.get('expansion_packs_b?'),
                combined_data.get('expansion_packs_c?')
            ]

            cleanup_has_expansion = (
                (expansion_packs and int(expansion_packs) > 0) or
                any(val and int(val or 0) > 0 for val in alt_expansion_indicators if val is not None) or
                combined_data.get('exp_1_soc') is not None or
                combined_data.get('exp_1_type') is not None
            )

            if not cleanup_has_expansion:
                logger.info(f"🧹 EXPANSION DEBUG - Cleaning up expansion sensors")
                logger.info(f"🧹 Cleanup detection fields: {cleanup_expansion_fields}")
                self._cleanup_expansion_sensors(device_sn)
            else:
                logger.info(f"🔋 EXPANSION DEBUG - Keeping expansion sensors (expansion detected during cleanup check)")

            self._cleanup_done.add(cleanup_key)

        # Publish updated state with combined data
        logger.debug(f"📤 Publishing state update with {len(combined_data)} total fields")
        logger.debug(f"🔋 Sample values: battery_soc={combined_data.get('battery_soc', 'N/A')}, ac_output_power={combined_data.get('ac_output_power', 'N/A')}, temperature={combined_data.get('temperature', 'N/A')}")
        self.publish_device_state(device_sn, combined_data, {})

    def _get_sensor_definitions(self, device_type: str) -> dict:
        """Get all sensor definitions for a device type."""
        from api.apitypes import SolixDeviceType

        # Common sensors for all devices
        common_sensors = {
            "wifi_signal": {
                "unit_of_measurement": "%",
                "value_template": "{{ value_json.wifi_signal }}",
                "icon": "mdi:wifi",
                "state_class": "measurement",
                "entity_category": "diagnostic"
            },
            "status": {
                "value_template": "{{ value_json.status_desc }}",
                "icon": "mdi:information",
                "entity_category": "diagnostic"
            }
        }

        # Device-specific sensors
        device_sensors = {}

        if device_type == SolixDeviceType.PPS.value:
            device_sensors.update({
                "battery_soc": {
                    "device_class": "battery",
                    "unit_of_measurement": "%",
                    "value_template": "{{ value_json.battery_soc }}",
                    "state_class": "measurement"
                },
                "battery_energy": {
                    "device_class": "energy",
                    "unit_of_measurement": "Wh",
                    "value_template": "{{ value_json.battery_energy }}",
                    "state_class": "measurement",
                    "icon": "mdi:battery"
                },
                "battery_size": {
                    "device_class": "energy",
                    "unit_of_measurement": "Wh",
                    "value_template": "{{ value_json.battery_size }}",
                    "icon": "mdi:battery-capacity-outline"
                },
                "temperature": {
                    "device_class": "temperature",
                    "unit_of_measurement": "°C",
                    "value_template": "{{ value_json.temperature }}",
                    "state_class": "measurement"
                },
                "ac_output_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.ac_output_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:power-plug"
                },
                "ac_output_power_total": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.ac_output_power_total }}",
                    "state_class": "measurement",
                    "icon": "mdi:power-plug"
                },
                "dc_input_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.dc_input_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:solar-panel"
                },
                "grid_to_battery_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.grid_to_battery_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:battery-charging"
                },
                "usbc_1_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.usbc_1_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:usb"
                },
                "usbc_2_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.usbc_2_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:usb"
                },
                "usba_1_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.usba_1_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:usb"
                },
                "usba_2_power": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.usba_2_power }}",
                    "state_class": "measurement",
                    "icon": "mdi:usb"
                },
                "battery_soh": {
                    "unit_of_measurement": "%",
                    "value_template": "{{ value_json.battery_soh }}",
                    "state_class": "measurement",
                    "icon": "mdi:battery-heart"
                },
                "rssi": {
                    "device_class": "signal_strength",
                    "unit_of_measurement": "dBm",
                    "value_template": "{{ value_json.rssi }}",
                    "state_class": "measurement",
                    "icon": "mdi:wifi-strength-2",
                    "entity_category": "diagnostic"
                },
                "max_load": {
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "value_template": "{{ value_json.max_load }}",
                    "icon": "mdi:speedometer"
                },
                "battery_size": {
                    "device_class": "energy",
                    "unit_of_measurement": "Wh",
                    "value_template": "{{ value_json.battery_size }}",
                    "icon": "mdi:battery-capacity-outline",
                    "entity_category": "diagnostic"
                },
                "device_timeout_minutes": {
                    "device_class": "duration",
                    "unit_of_measurement": "min",
                    "value_template": "{{ value_json.device_timeout_minutes }}",
                    "icon": "mdi:timer-off",
                    "entity_category": "config"
                },
                "display_timeout_seconds": {
                    "device_class": "duration",
                    "unit_of_measurement": "s",
                    "value_template": "{{ value_json.display_timeout_seconds }}",
                    "icon": "mdi:monitor-off",
                    "entity_category": "config"
                }
            })

        return {**common_sensors, **device_sensors}

    async def initialize_api(self, email: str, password: str, country: str, websession: ClientSession):
        """Initialize the Anker API."""
        api_logger = logging.getLogger(f"{__name__}.api")
        api_logger.setLevel(logging.INFO)

        self.api = AnkerSolixApi(
            email, password, country, websession, api_logger
        )

        if await self.api.async_authenticate():
            logger.info("Anker Cloud authentication: OK")
            return True
        else:
            logger.info("Anker Cloud authentication: CACHED")
            return True

    async def publish_all_devices(self, site_id: str = None, enable_mqtt: bool = False):
        """Publish data for all devices."""
        if not self.api:
            logger.error("API not initialized")
            return False

        try:
            # Update sites and devices
            logger.info("Updating sites...")
            await self.api.update_sites(siteId=site_id)

            logger.info("Updating device details...")
            await self.api.update_device_details()

            # Optionally enable MQTT session for real-time data
            mqtt_session = None
            if enable_mqtt:
                logger.info("Starting MQTT session...")
                mqtt_session = await self.api.startMqttSession()
                if mqtt_session:
                    # Subscribe to devices
                    logger.info("🔍 DEBUG - MQTT Session Details:")
                    logger.info(f"   📋 MQTT session type: {type(mqtt_session)}")
                    logger.info(f"   🔌 Is connected: {mqtt_session.is_connected() if hasattr(mqtt_session, 'is_connected') else 'Unknown'}")
                    if hasattr(mqtt_session, '__dict__'):
                        logger.info(f"   📊 Session attributes: {list(mqtt_session.__dict__.keys())}")

                    for dev in self.api.devices.values():
                        if dev.get("mqtt_described"):
                            device_name = dev.get('name', 'Unknown')
                            device_sn = dev.get('device_sn', 'Unknown')
                            topic_prefix = mqtt_session.get_topic_prefix(deviceDict=dev)
                            topic = f"{topic_prefix}#"

                            logger.info(f"🔍 DEBUG - Subscribing to device {device_name}:")
                            logger.info(f"   📍 Topic prefix: {topic_prefix}")
                            logger.info(f"   🎯 Full topic: {topic}")

                            resp = mqtt_session.subscribe(topic)

                            logger.info(f"   📋 Subscription response: {type(resp) if resp else None}")
                            if resp:
                                logger.info(f"   ✅ Success: {not resp.is_failure if hasattr(resp, 'is_failure') else 'Unknown'}")
                                if hasattr(resp, '__dict__'):
                                    logger.info(f"   📊 Response attributes: {list(resp.__dict__.keys())}")

                            if resp and resp.is_failure:
                                logger.warning(f"Failed subscription for topic: {topic}")
                    logger.info("MQTT session established")
                else:
                    logger.warning("Failed to start MQTT session")

            # Process each device
            for device_sn, device_info in self.api.devices.items():
                if site_id and device_info.get("site_id") != site_id:
                    continue

                logger.info(f"Publishing device: {device_info.get('name', device_sn)}")
                logger.info(f"  📋 Device type: {device_info.get('type', 'unknown')}")
                logger.info(f"  🏷️ Model: {device_info.get('device_pn', 'Unknown')}")

                # Create basic sensors from device info (wifi_signal, battery_capacity, etc.)
                self.create_sensor_entities(device_sn, device_info, device_info)
                self.create_binary_sensor_entities(device_sn, device_info, device_info)
                self.create_switch_entities(device_sn, device_info, device_info)

                # Track initial entities
                initial_sensors = self._get_sensor_definitions(device_info.get('type', ''))
                basic_entities = set()
                for sensor_name in initial_sensors:
                    if sensor_name in device_info or sensor_name == 'wifi_signal':
                        basic_entities.add(sensor_name)

                self.device_entities[device_sn] = basic_entities

                # Get MQTT data if available and create additional sensors
                mqtt_data = device_info.get("mqtt_data", {}) if mqtt_session else {}
                if mqtt_data:
                    logger.info(f"📡 Found existing MQTT data with {len(mqtt_data)} fields - creating sensors now")
                    self.update_sensors_from_mqtt_data(device_sn, device_info)
                else:
                    logger.info("📡 No MQTT data yet - will create sensors when data arrives")
                    # Publish current state
                    self.publish_device_state(device_sn, device_info, mqtt_data)

            logger.info(f"Published {len(self.api.devices)} devices to MQTT")
            return True

        except Exception as e:
            logger.error(f"Error publishing devices: {e}")
            return False

    def mqtt_message_callback(self, mqtt_session, topic, message, data, model, device_sn, valueupdate):
        """Callback for MQTT message updates - publishes immediately to Home Assistant."""
        topic_name = topic.split('/')[-1] if '/' in topic else topic
        logger.info(f"📡 MQTT callback: topic={topic_name}, device={device_sn}, valueupdate={valueupdate}")
        logger.debug(f"📋 Full topic: {topic}")

        # ENHANCED DEBUGGING: Log raw message details
        logger.info(f"🔍 DEBUG - Raw MQTT message details:")
        logger.info(f"   📍 Full topic: {topic}")
        logger.info(f"   📦 Message type: {type(message)}")
        logger.info(f"   📊 Data type: {type(data)}")
        logger.info(f"   🔢 Model: {model}")
        logger.info(f"   🆔 Device SN: {device_sn}")
        logger.info(f"   🔄 Value update flag: {valueupdate}")

        # Log raw message content if available
        if hasattr(message, 'payload'):
            try:
                payload_str = message.payload.decode('utf-8', errors='replace') if isinstance(message.payload, bytes) else str(message.payload)
                logger.info(f"   📥 Raw payload: {payload_str[:200]}{'...' if len(payload_str) > 200 else ''}")
            except Exception as e:
                logger.info(f"   📥 Raw payload decode error: {e}")

        # Log what data we received
        if hasattr(data, 'keys') and len(data) > 0:
            logger.info(f"📊 Received {len(data)} fields from {topic_name}: {list(data.keys())}")
            # Show ALL data for debugging (truncated)
            if len(data) <= 20:
                logger.info(f"🔋 All data from {topic_name}: {dict(data)}")
            else:
                logger.info(f"🔋 First 20 fields from {topic_name}: {dict(list(data.items())[:20])}")
            # Show sample values for important fields
            sample_fields = ['battery_soc', 'ac_output_power', 'temperature', 'dc_input_power', 'grid_to_battery_power']
            sample_data = {k: data.get(k) for k in sample_fields if k in data}
            if sample_data:
                logger.info(f"🔋 Key sample values from {topic_name}: {sample_data}")
        else:
            logger.info(f"📊 Received empty data from {topic_name}")
            # Log if data exists but in different format
            if data is not None:
                logger.info(f"   🔍 Data is not None but not dict-like: {type(data)} = {str(data)[:100]}")

        # Also log the raw MQTT session cache data for comparison
        if device_sn and self.api.mqttsession and hasattr(self.api.mqttsession, 'mqtt_data'):
            session_data = self.api.mqttsession.mqtt_data.get(device_sn, {})
            if session_data:
                logger.info(f"🗄️ Session cache has {len(session_data)} fields")
                session_sample = {k: session_data.get(k) for k in ['ac_output_power', 'usbc_1_power', 'battery_soc'] if k in session_data}
                logger.info(f"🗄️ Session cache sample: {session_sample}")
            else:
                logger.info(f"🗄️ Session cache is empty for device {device_sn}")
        else:
            logger.info(f"🗄️ No session cache available")

        if device_sn:
            try:
                # Get updated device info from API cache
                if device_info := self.api.devices.get(device_sn):
                    logger.info(f"🔄 Real-time MQTT update for {device_info.get('name', device_sn)} (topic: {topic_name})")
                    # Update sensors and publish state immediately - process all MQTT messages, not just valueupdate=True
                    self.update_sensors_from_mqtt_data(device_sn, device_info)
                else:
                    logger.debug(f"📡 MQTT update for unknown device: {device_sn}")
            except Exception as e:
                logger.error(f"Error handling MQTT callback for {device_sn}: {e}")
        else:
            logger.debug(f"📡 MQTT message: topic={topic}, no device_sn identified")

    async def run_continuous(self, interval: int = 30, site_id: str = None,
                           enable_mqtt: bool = False, enable_realtime: bool = False):
        """Run continuous monitoring and publishing."""
        if enable_mqtt:
            logger.info("Starting event-driven MQTT monitoring (real-time updates)")
        else:
            logger.info(f"Starting polling-based monitoring (interval: {interval}s)")

        # Initial device discovery and setup
        await self.publish_all_devices(site_id=site_id, enable_mqtt=enable_mqtt)

        # Create keep-alive control switch
        self.create_keepalive_switch()

        # Setup MQTT callback and realtime triggers if enabled
        if enable_mqtt and self.api.mqttsession:
            # Set up the MQTT callback for real-time updates
            logger.info("Setting up MQTT callback for real-time updates...")
            self.api.mqttsession.message_callback(self.mqtt_message_callback)

            if enable_realtime:
                logger.info("Enabling realtime MQTT triggers...")
                # Give MQTT client time to fully connect and subscribe
                await asyncio.sleep(5)

                # Wait for MQTT client to be properly connected
                max_wait = 10  # seconds
                wait_time = 0
                while wait_time < max_wait:
                    if self.api.mqttsession.is_connected():
                        logger.info("✅ MQTT session is connected, proceeding with triggers")
                        break
                    logger.debug(f"⏳ Waiting for MQTT connection... ({wait_time}s/{max_wait}s)")
                    await asyncio.sleep(1)
                    wait_time += 1

                if not self.api.mqttsession.is_connected():
                    logger.warning("❌ MQTT session not connected after waiting, skipping realtime triggers")
                    # Continue without realtime triggers, callback will still work
                else:
                    mqtt_devices = [dev for dev in self.api.devices.values() if dev.get("mqtt_described")]
                    if not mqtt_devices:
                        logger.warning("No devices support MQTT realtime triggers")
                    else:
                        for dev in mqtt_devices:
                            device_name = dev.get('name', 'Unknown')
                            device_sn = dev.get('device_sn', 'Unknown')
                            logger.info(f"🔔 Attempting realtime trigger for {device_name} ({device_sn})")

                            try:
                                if not self.api.mqttsession.is_connected():
                                    logger.warning("MQTT session not connected, skipping realtime trigger")
                                    continue

                                # Check if device supports MQTT
                                mqtt_supported = dev.get("mqtt_supported", False)
                                mqtt_described = dev.get("mqtt_described", False)
                                logger.info(f"🔍 Device {device_name}: mqtt_supported={mqtt_supported}, mqtt_described={mqtt_described}")

                                if not mqtt_described:
                                    logger.warning(f"⚠️ Device {device_name} doesn't support MQTT realtime triggers")
                                    continue

                                # Wait a bit more for MQTT client to be fully ready
                                await asyncio.sleep(1)

                                # Send display wake-up command for C1000X devices first
                                if dev.get('device_pn') == 'A1761':  # C1000X
                                    logger.info(f"💡 Sending display wake-up command for {device_name}...")
                                    try:
                                        device = create_mqtt_device(self.api, device_sn)
                                        if device:
                                            wake_result = await device.set_display(enabled=True)
                                            if wake_result:
                                                logger.info(f"✅ Display wake-up successful for {device_name}")
                                            else:
                                                logger.warning(f"⚠️ Display wake-up failed for {device_name}")
                                            # Give device time to process wake-up command
                                            await asyncio.sleep(2)
                                        else:
                                            logger.warning(f"⚠️ Could not create device instance for wake-up: {device_name}")
                                    except Exception as e:
                                        logger.warning(f"⚠️ Display wake-up error for {device_name}: {e}")

                                logger.info(f"📡 Sending realtime trigger for {device_name}...")
                                # DEBUG: Log what we're about to send
                                logger.info(f"🔍 DEBUG - Realtime trigger details:")
                                logger.info(f"   🎯 Target device: {device_name} ({device_sn})")
                                logger.info(f"   🏷️ Device PN: {dev.get('device_pn')}")
                                logger.info(f"   📡 MQTT described: {dev.get('mqtt_described')}")
                                logger.info(f"   🔌 MQTT supported: {dev.get('mqtt_supported')}")
                                logger.info(f"   ⏱️ Timeout: 60s")

                                resp = self.api.mqttsession.realtime_trigger(
                                    deviceDict=dev, timeout=60
                                )

                                # DEBUG: Log the response
                                logger.info(f"🔍 DEBUG - Realtime trigger response:")
                                logger.info(f"   📋 Response object: {type(resp)}")
                                logger.info(f"   ✅ Response exists: {resp is not None}")
                                if resp:
                                    logger.info(f"   📤 Is published: {resp.is_published() if hasattr(resp, 'is_published') else 'No is_published method'}")
                                    if hasattr(resp, '__dict__'):
                                        logger.info(f"   📊 Response attributes: {list(resp.__dict__.keys())}")
                                    if hasattr(resp, 'topic'):
                                        logger.info(f"   📍 Sent to topic: {resp.topic}")
                                    if hasattr(resp, 'payload'):
                                        logger.info(f"   📦 Payload: {str(resp.payload)[:100]}")
                                else:
                                    logger.warning(f"   ❌ No response object returned")

                                if resp:
                                    # Wait for publish to complete
                                    await asyncio.sleep(0.5)
                                    if resp.is_published():
                                        logger.info(f"✅ Successfully triggered realtime for {device_name}")
                                    else:
                                        logger.warning(f"❌ Realtime trigger failed for {device_name}: Not published (check MQTT permissions)")
                                else:
                                    logger.warning(f"❌ Realtime trigger failed for {device_name}: No response object")
                            except Exception as e:
                                logger.warning(f"❌ Realtime trigger error for {device_name}: {e}")

        # Continuous loop
        try:
            if enable_mqtt:
                # Event-driven mode: just keep the session alive, updates come via callback
                logger.info("🔄 MQTT session active - updates will be published in real-time")
                logger.info("   Use Ctrl+C to stop...")

                # Shorter polling for command processing, longer for health checks
                command_check_interval = 30  # Check for stale commands every 30 seconds
                health_check_interval = max(300, interval * 10)  # At least 5 minutes, or 10x the original interval
                logger.info(f"📅 Command check every {command_check_interval}s, health check every {health_check_interval}s")

                # Initialize health check timestamp and command event
                self._last_health_check = asyncio.get_event_loop().time()
                self._command_event = asyncio.Event()  # Create event in async context
                logger.debug("📧 Command event created for real-time command processing")

                while True:
                    try:
                        # Wait for commands or timeout for command checking
                        logger.debug(f"🎛️ Waiting for commands (event is_set={self._command_event.is_set()}, pending={len(self._pending_commands)})")
                        await asyncio.wait_for(self._command_event.wait(), timeout=command_check_interval)
                        # Command received - process all pending commands
                        logger.info("🎛️ Command event triggered - processing pending commands")
                        await self._process_pending_commands()
                        # Continue the loop to wait for next event or timeout
                        continue
                    except asyncio.TimeoutError:
                        # Timeout reached - check for stale commands first
                        current_time = asyncio.get_event_loop().time()

                        # Check for stale commands and restart MQTT session if needed
                        await self._check_and_recover_stale_commands()

                        # Process any pending commands that might have arrived
                        if self._pending_commands:
                            logger.info("🎛️ Processing pending commands during timeout check")
                            await self._process_pending_commands()

                        # Check if it's time for a full health check
                        time_since_health_check = current_time - self._last_health_check
                        if time_since_health_check >= health_check_interval:
                            logger.debug(f"⏰ Health check timeout reached (pending commands: {len(self._pending_commands)})")
                            await self.api.update_sites(siteId=site_id)
                            self._last_health_check = current_time

                            # Send keep-alive triggers if enabled
                            await self._handle_keepalive_triggers()

                        # Only publish fallback state during health check
                        for device_sn, device_info in self.api.devices.items():
                            if site_id and device_info.get("site_id") != site_id:
                                continue

                            mqtt_data = device_info.get("mqtt_data", {})
                            if not mqtt_data:
                                # Publish basic state for devices without MQTT data
                                logger.debug(f"📤 Publishing fallback state for device without MQTT data: {device_sn}")
                                self.publish_device_state(device_sn, device_info, mqtt_data)
            else:
                # Polling mode: traditional interval-based updates
                logger.info(f"📊 Polling mode - updating every {interval} seconds")

                # Initialize data refresh timestamp and command event
                self._last_data_refresh = asyncio.get_event_loop().time()
                self._command_event = asyncio.Event()  # Create event in async context
                logger.debug("📧 Command event created for real-time command processing")

                while True:
                    try:
                        # Wait for commands or timeout for data refresh
                        await asyncio.wait_for(self._command_event.wait(), timeout=interval)
                        # Command received - process immediately
                        await self._process_pending_commands()
                    except asyncio.TimeoutError:
                        # Timeout reached - do data refresh
                        logger.debug("Refreshing device data...")
                        await self.api.update_sites(siteId=site_id)
                        self._last_data_refresh = asyncio.get_event_loop().time()

                        # Check for new MQTT data and create sensors dynamically
                        for device_sn, device_info in self.api.devices.items():
                            if site_id and device_info.get("site_id") != site_id:
                                continue

                            mqtt_data = device_info.get("mqtt_data", {})

                            # If we have new MQTT data, update sensors
                            if mqtt_data:
                                self.update_sensors_from_mqtt_data(device_sn, device_info)
                            else:
                                # Just publish state if no MQTT data
                                self.publish_device_state(device_sn, device_info, mqtt_data)

        except KeyboardInterrupt:
            logger.info("Stopping continuous monitoring...")
        except Exception as e:
            logger.error(f"Error in continuous monitoring: {e}")
        finally:
            if self.api and self.api.mqttsession:
                logger.info("Stopping MQTT session...")
                self.api.stopMqttSession()


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Home Assistant MQTT Publisher for Anker Solix devices"
    )

    # Credentials and configuration
    parser.add_argument("--env-file", type=str,
                       help="Path to .env file for credentials (default: .env)")

    # Operation modes
    parser.add_argument("--live", action="store_true",
                       help="Use live cloud data")
    parser.add_argument("--mqtt", action="store_true",
                       help="Enable MQTT session for real-time device data (event-driven updates)")
    parser.add_argument("--rt", "--realtime", action="store_true",
                       help="Enable real-time MQTT trigger to request immediate data (requires --mqtt)")
    parser.add_argument("--interval", "-i", type=int, default=30,
                       help="Polling interval in seconds when MQTT is disabled, or fallback interval when MQTT is enabled (default: 30)")
    parser.add_argument("--site-id", type=str,
                       help="Monitor specific site ID only")
    parser.add_argument("--no-ev", action="store_true",
                       help="Disable electric vehicles")

    # MQTT broker settings
    parser.add_argument("--mqtt-host", default="localhost",
                       help="MQTT broker host (default: localhost)")
    parser.add_argument("--mqtt-port", type=int, default=1883,
                       help="MQTT broker port (default: 1883)")
    parser.add_argument("--mqtt-user",
                       help="MQTT broker username")
    parser.add_argument("--mqtt-password",
                       help="MQTT broker password")
    parser.add_argument("--ha-prefix", default="homeassistant",
                       help="Home Assistant discovery prefix (default: homeassistant)")
    parser.add_argument("--device-prefix", default="anker_solix_mqtt",
                       help="Device prefix for MQTT topics (default: anker_solix_mqtt)")

    # Run mode
    parser.add_argument("--once", action="store_true",
                       help="Run once and exit (default: continuous)")

    args = parser.parse_args()

    # Validate argument combinations
    if args.rt and not args.mqtt:
        parser.error("--realtime requires --mqtt to be specified")

    return args


async def main():
    """Main function."""
    args = parse_arguments()

    # Load credentials from environment or .env file
    credentials = load_credentials(args.env_file)
    if not validate_required_credentials(credentials):
        return False

    # Override MQTT settings from credentials if not specified via command line
    mqtt_host = args.mqtt_host
    mqtt_port = args.mqtt_port
    mqtt_user = args.mqtt_user
    mqtt_password = args.mqtt_password

    # Use credentials from env/file if command line args not provided
    if mqtt_host == "localhost" and credentials.get('mqtt_host'):
        mqtt_host = credentials['mqtt_host']
    if mqtt_port == 1883 and credentials.get('mqtt_port'):
        try:
            mqtt_port = int(credentials['mqtt_port'])
        except ValueError:
            logger.warning(f"Invalid MQTT port in credentials: {credentials['mqtt_port']}")
    if not mqtt_user and credentials.get('mqtt_user'):
        mqtt_user = credentials['mqtt_user']
    if not mqtt_password and credentials.get('mqtt_password'):
        mqtt_password = credentials['mqtt_password']

    # Initialize publisher
    logger.info(f"🏷️ Using device prefix: {args.device_prefix}")
    publisher = HomeAssistantMqttPublisher(
        mqtt_host=mqtt_host,
        mqtt_port=mqtt_port,
        mqtt_user=mqtt_user,
        mqtt_password=mqtt_password,
        ha_discovery_prefix=args.ha_prefix,
        device_prefix=args.device_prefix
    )

    # Connect to MQTT broker
    mqtt_connected = publisher.connect_mqtt()
    if not mqtt_connected:
        logger.warning("Failed to connect to MQTT broker - continuing without MQTT publishing")
        logger.warning("Check your MQTT broker settings in .env file or command line arguments")

    # Create persistent session for the entire run
    async with ClientSession() as websession:
        try:
            # Initialize API with credentials from env/file
            email = credentials['email']
            password = credentials['password']
            country = credentials['country']

            logger.info(f"Initializing Anker API for user: {email}")
            if not await publisher.initialize_api(email, password, country, websession):
                logger.error("Failed to initialize Anker API")
                return False

            # Run publisher
            if args.once:
                logger.info("Running once...")
                await publisher.publish_all_devices(
                    site_id=args.site_id,
                    enable_mqtt=args.mqtt
                )
            else:
                logger.info("Starting continuous monitoring...")
                await publisher.run_continuous(
                    interval=args.interval,
                    site_id=args.site_id,
                    enable_mqtt=args.mqtt,
                    enable_realtime=args.rt
                )

            return True

        except Exception as e:
            logger.error(f"Error in main execution: {e}")
            return False
        finally:
            if publisher.mqtt_client:
                publisher.mqtt_client.loop_stop()
                publisher.mqtt_client.disconnect()


if __name__ == "__main__":
    try:
        result = asyncio.run(main())
        if not result:
            exit(1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        exit(1)
