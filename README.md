# Home Assistant MQTT Publisher for Anker Solix Devices

⚠️ **For Testing Purposes Only** - For production use, please use the official [ha-anker-solix](https://github.com/thomluther/hacs-anker-solix) integration.

This Docker container publishes data from Anker Solix devices to Home Assistant via MQTT discovery.

![Home Assistant Integration](screenshot.png)
*Example of the automatic device integration with control switches and sensors*

## Quick Start

1. Copy the environment template:
```bash
cp .env.example .env
```

2. Edit `.env` with your credentials:
```bash
# Edit the .env file with your actual values
nano .env
```

3. Run with docker-compose:
```bash
docker-compose up -d
```

4. Check logs:
```bash
docker-compose logs -f
```

## Features

- Real-time MQTT updates (event-driven, not polling)
- Home Assistant MQTT discovery (devices auto-appear)
- Device control switches (AC output, DC output, backup charge, ultrafast charging)
- Proper entity categorization (main sensors vs diagnostic vs config)
- Automatic cleanup of unused sensors
- Non-interactive authentication

## Requirements

- Anker Solix device with MQTT support (e.g., C1000X)
- MQTT broker accessible from the container
- Home Assistant with MQTT integration enabled