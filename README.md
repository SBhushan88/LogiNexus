# LogiNexus: Integrated Logistics Streaming & Analytics Platform

## 1. Project Overview

LogiNexus is a real-time logistics streaming and analytics platform developed to demonstrate how streaming technologies can support logistics monitoring, operational visibility, and risk identification.

The platform integrates vehicle, shipment, warehouse, delivery, and weather data streams. It uses Apache Kafka for event streaming, Python for data production and processing, MongoDB for operational data storage, MySQL for dashboard-ready data, and Grafana for visualisation.

Operational logistics events are simulated for this academic project, while weather information is retrieved from the Open-Meteo API.

## 2. Objectives

- Monitor vehicle movement and operational status.
- Track shipment progress and identify delays.
- Monitor warehouse utilisation and congestion.
- Analyse delivery outcomes and failure reasons.
- Incorporate weather information into logistics risk monitoring.
- Generate risk alerts using rule-based scoring.
- Visualise operational indicators through a central Grafana dashboard.

## 3. Technology Stack

- **Programming language:** Python
- **Streaming platform:** Apache Kafka
- **Operational database:** MongoDB
- **Dashboard database:** MySQL
- **Visualisation:** Grafana
- **Containerisation:** Docker
- **Weather data source:** Open-Meteo API
- **Development environment:** Visual Studio Code

## 4. System Architecture

The project follows this data flow:

Python Producers and Open-Meteo API  
↓  
Apache Kafka  
↓  
Python Consumers and Stream Processing  
↓  
MongoDB and MySQL  
↓  
Grafana Dashboard

Python producers generate simulated logistics events and retrieve weather information. Kafka distributes these events through dedicated topics. Python processing modules analyse incoming events and generate risk alerts. MongoDB stores operational records, while MySQL provides structured data for Grafana visualisations.

## 5. Kafka Topics

| Topic | Purpose |
|---|---|
| `vehicle-location` | Vehicle movement and status events |
| `shipment-events` | Shipment progress and delay events |
| `warehouse-events` | Warehouse utilisation and congestion events |
| `delivery-events` | Delivery outcomes and failure events |
| `weather-events` | Weather observations |
| `logistics-alerts` | Generated logistics risk alerts |

## 6. Data Sources

The project uses simulated operational data for vehicles, shipments, warehouses, and deliveries. Weather information is retrieved from the Open-Meteo API for the configured locations:

- Delhi
- Gurgaon
- Jaipur
- Chandigarh
- Agra
- Lucknow

Operational event volumes and frequencies depend on the producer configurations and the duration of execution.

## 7. Risk Scoring

The project implements a rule-based risk-scoring model. The following weights are defined for the academic implementation:

| Risk condition | Score contribution |
|---|---:|
| Delayed shipment | +30 |
| Severe weather | +25 |
| Idle vehicle | +20 |
| Route deviation | +20 |
| Low vehicle speed | +15 |
| Warehouse congestion | +15 |

The risk score is capped at 100.

| Risk score | Classification |
|---|---|
| 0–30 | LOW |
| 31–60 | MEDIUM |
| 61–100 | HIGH |

These weights and thresholds are project-defined rules and are not official industry standards.

## 8. Project Structure

The main project directories are:

- `producers/` — Python scripts that generate or retrieve incoming events.
- `consumers/` — Consumers that read Kafka topics and store operational records.
- `processing/` — Stream processing, risk calculations, window processing, and dashboard data writing.
- `data/` — Project data files, where applicable.
- `database/` — Database-related scripts and resources, where applicable.

## 9. Setup Requirements

Install or configure the following tools before running the project:

- Python
- Docker Desktop
- Apache Kafka and its required supporting services
- MongoDB
- MySQL
- Grafana
- Required Python packages

The project uses environment variables for local configuration. Create a local `.env` file using the configuration required by the application. Do not upload credentials, passwords, or private connection strings to GitHub.

## 10. Running the Project

1. Open the project folder in Visual Studio Code.
2. Start the required Docker containers and supporting services.
3. Activate the project's Python virtual environment.
4. Install the required Python dependencies.
5. Configure the local environment variables.
6. Start the Kafka producers.
7. Start the required consumers and processing scripts.
8. Confirm that operational records and dashboard data are being updated.
9. Open Grafana and view the LogiNexus dashboard.

Use the actual scripts and configuration files in the repository to determine the required startup order and commands.

## 11. Dashboard and Analytics

The Grafana dashboard provides operational and analytical views, including:

- Total vehicles
- Active shipments
- Delayed shipments
- Total warehouses
- High-risk alerts
- Vehicle status distribution
- Shipment status distribution
- Warehouse utilisation and congestion
- Delivery outcomes
- Failed deliveries and reasons
- Priority risk alerts
- Weather impact by location
- Risk trends over time

The dashboard helps demonstrate how streaming data can support operational monitoring and risk prioritisation.

## 12. Limitations

This is an academic demonstration rather than a production logistics system. Vehicle, shipment, warehouse, and delivery events are simulated. Weather observations are retrieved from an external API. Risk scores depend on project-defined rules, and the results should not be interpreted as verified performance measurements from a real logistics company.

## 13. Academic Project Disclosure

Due to the lack of an accessible real-time public logistics transaction feed suitable for this academic implementation, operational logistics events were simulated. External environmental data was integrated from the Open-Meteo API.

## 14. Repository

Source code and project resources are available in this repository:

https://github.com/SBhushan88/LogiNexus