def calculate_risk_score(
    shipment_delayed=False,
    heavy_weather=False,
    vehicle_idle=False,
    route_deviation=False,
    low_vehicle_speed=False,
    warehouse_congested=False
):
    score = 0
    reasons = []

    if shipment_delayed:
        score += 30
        reasons.append("Shipment delayed")

    if heavy_weather:
        score += 25
        reasons.append("Severe weather")

    if vehicle_idle:
        score += 20
        reasons.append("Vehicle idle")

    if route_deviation:
        score += 20
        reasons.append("Route deviation")

    if low_vehicle_speed:
        score += 15
        reasons.append("Low vehicle speed")

    if warehouse_congested:
        score += 15
        reasons.append("Warehouse congestion")

    score = min(score, 100)

    if score <= 30:
        risk_level = "LOW"

    elif score <= 60:
        risk_level = "MEDIUM"

    else:
        risk_level = "HIGH"

    return {
        "risk_score": score,
        "risk_level": risk_level,
        "reasons": reasons
    }